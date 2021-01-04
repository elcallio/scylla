/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <algorithm>
#include "service_level_controller.hh"
#include "service/storage_service.hh"
#include "service/priority_manager.hh"
#include "message/messaging_service.hh"
#include "db/system_distributed_keyspace.hh"
#include <seastar/core/reactor.hh>

namespace qos {
logging::logger sl_logger("workload prioritization");

sstring service_level_controller::default_service_level_name = "default";



service_level_controller::service_level_controller(sharded<auth::service>& auth_service, service_level_options default_service_level_config)
        : _sl_data_accessor(nullptr)
        , _auth_service(auth_service)
{
    if (this_shard_id() == global_controller) {
        _global_controller_db = std::make_unique<global_controller_data>();
        _global_controller_db->default_service_level_config = default_service_level_config;
    }
}

future<> service_level_controller::add_service_level(sstring name, service_level_options slo, bool is_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, slo, is_static] () {
           return sl_controller.do_add_service_level(name, slo, is_static);
        });
    });
}

future<>  service_level_controller::remove_service_level(sstring name, bool remove_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, remove_static] () {
           return sl_controller.do_remove_service_level(name, remove_static);
        });
    });
}

future<> service_level_controller::start() {
    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }
    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this] () {
        return do_add_service_level(default_service_level_name, _global_controller_db->default_service_level_config, true).then([this] () {
            return container().invoke_on_all([] (service_level_controller& sl) {
                sl._default_service_level =  sl.get_service_level(default_service_level_name);
                sl.register_with_priority_manager();
            });
        });
    });
}


void service_level_controller::set_distributed_data_accessor(service_level_distributed_data_accessor_ptr sl_data_accessor) {
    // unregistering the accessor is always legal
    if (!sl_data_accessor) {
        _sl_data_accessor = nullptr;
    }

    // Registration of a new accessor can be done only when the _sl_data_accessor is not already set.
    // This behavior is intended to allow to unit testing debug to set this value without having
    // overriden by storage_proxy
    if (!_sl_data_accessor) {
        _sl_data_accessor = sl_data_accessor;
    }
}

future<> service_level_controller::stop() {
    // unregister from the service level distributed data accessor.
    _sl_data_accessor = nullptr;
    //unregister from the priority manager
    service::get_local_priority_manager().set_service_level_controller(nullptr);
    if (this_shard_id() == global_controller) {
        // abort the loop of the distributed data checking if it is running
        _global_controller_db->dist_data_update_aborter.request_abort();
        _global_controller_db->notifications_serializer.broken();
        return _global_controller_db->distributed_data_update.then([this] {
            return ::parallel_for_each(_service_levels_db.begin(), _service_levels_db.end(), [this] (auto&& sl_record) {
                return ::destroy_scheduling_group(sl_record.second.sg);
            });
        }).then([this] () {
            return ::do_until([this] () { return _global_controller_db->deleted_scheduling_groups.empty(); }, [this] () {
                return ::destroy_scheduling_group(_global_controller_db->deleted_scheduling_groups.top()).then([this] () {
                    _global_controller_db->deleted_scheduling_groups.pop();
                });
            });
        });
    }
    return make_ready_future();
}

void service_level_controller::register_with_priority_manager() {
    service::get_local_priority_manager().set_service_level_controller(this);
}

future<> service_level_controller::update_service_levels_from_distributed_data() {

    if (!_sl_data_accessor) {
        return make_ready_future();
    }

    return container().invoke_on(global_controller, [] (service_level_controller& sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller] () {
            return async([&sl_controller] () {
                service_levels_info service_levels;
                try {
                    service_levels = sl_controller._sl_data_accessor->get_service_levels().get0();
                } catch (...) {
                    sl_logger.warn("update_service_levels_from_distributed_data: an error occurred"
                            " while retrieving configuration ({})", std::current_exception());
                    return;
                }
                service_levels_info service_levels_for_add_or_update;
                service_levels_info service_levels_for_delete;

                auto current_it = sl_controller._service_levels_db.begin();
                auto new_state_it = service_levels.begin();

                // we want to detect 3 kinds of objects in one pass -
                // 1. new service levels that have been added to the distributed keyspace
                // 2. existing service levels that have changed
                // 3. removed service levels
                // this loop is batching together add/update operation and remove operation
                // then they are all executed together.The reason for this is to allow for
                // firstly delete all that there is to be deleted and only then adding new
                // service levels.
                while (current_it != sl_controller._service_levels_db.end() && new_state_it != service_levels.end()) {
                    if (current_it->first == new_state_it->first) {
                        //the service level exists on both the cureent and new state.
                       if (current_it->second.slo != new_state_it->second) {
                           // The service level configuration is different
                           // in the new state and the old state, meaning it needs to be updated.
                           sl_logger.info("service level \"{}\" was updated from {} to {} shares.",
                                   new_state_it->first.c_str(), current_it->second.slo.shares, new_state_it->second.shares);
                           service_levels_for_add_or_update.insert(*new_state_it);
                       }
                       current_it++;
                       new_state_it++;
                   } else if (current_it->first < new_state_it->first) {
                       //The service level does not exists in the new state so it needs to be
                       //removed, but only if it is not static since static configurations dont
                       //come from the distributed keyspace but from code.
                       if (!current_it->second.is_static) {
                           sl_logger.info("service level \"{}\" was deleted.", current_it->first.c_str());
                           service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                       }
                       current_it++;
                   } else { /*new_it->first < current_it->first */
                       // The service level exits in the new state but not in the old state
                       // so it needs to be added.
                       sl_logger.info("service level \"{}\" was added.", new_state_it->first.c_str());
                       service_levels_for_add_or_update.insert(*new_state_it);
                       new_state_it++;
                   }
                }

                for (; current_it != sl_controller._service_levels_db.end(); current_it++) {
                    if (!current_it->second.is_static) {
                        sl_logger.info("service level \"{}\" was deleted.", current_it->first.c_str());
                        service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                    }
                }
                for (; new_state_it != service_levels.end(); new_state_it++) {
                    sl_logger.info("service level \"{}\" was added.", new_state_it->first.c_str());
                    service_levels_for_add_or_update.emplace(new_state_it->first, new_state_it->second);
                }

                for (auto&& sl : service_levels_for_delete) {
                    sl_controller.do_remove_service_level(sl.first, false).get();
                }
                for (auto&& sl : service_levels_for_add_or_update) {
                    sl_controller.do_add_service_level(sl.first, sl.second).get();
                }
            });
        });
    });
}

future<sstring> service_level_controller::find_service_level(auth::role_set roles) {
    static auto sl_compare = [this] (const sstring& service_level1, const sstring& service_level2) {
        return _service_levels_db[service_level1].slo.shares <
                _service_levels_db[service_level2].slo.shares;
    };
    auto& role_manager = _auth_service.local().underlying_role_manager();

    // converts a list of roles into the chosen service level.
    return ::map_reduce(roles.begin(), roles.end(), [&role_manager, this] (const sstring& role) {
        return role_manager.get_attribute(role, "service_level").then_wrapped([this, role] (future<std::optional<sstring>> sl_name_fut) {
            try {
                std::optional<sstring> sl_name = sl_name_fut.get0();
                if (! sl_name) {
                    return sl_name;
                }
                auto sl_it = _service_levels_db.find(*sl_name);
                if ( sl_it == _service_levels_db.end()) {
                    return std::optional<sstring>{};
                } else {
                   return sl_name;
                }
            } catch(...) { // when we fail, we act as if the attribute does not exist so the node
                           // will not be brought down.
                return std::optional<sstring>{};
            }
        });
    }, std::optional<sstring>{}, [this] (std::optional<sstring> first, std::optional<sstring> second) {
        if (!second) {
            return first;
        } else if (!first) {
            return second;
        } else {
            return std::optional<sstring>{ sl_compare(*first, *second) ? second : first };
        }
    }).then([] (std::optional<sstring> sl) {
        return sl? *sl:default_service_level_name;
    });
}

future<>  service_level_controller::notify_service_level_added(sstring name, service_level sl_data) {
    auto result= _service_levels_db.emplace(name, sl_data);
    if (result.second) {
        unsigned sl_idx = internal::scheduling_group_index(sl_data.sg);
        _sl_lookup[sl_idx].first = &(result.first->first);
        _sl_lookup[sl_idx].second = &(result.first->second);
    }
    return make_ready_future();
}

future<> service_level_controller::notify_service_level_updated(sstring name, service_level_options slo) {
    auto sl_it = _service_levels_db.find(name);
    future<> f = make_ready_future();
    if (sl_it != _service_levels_db.end()) {
        if (sl_it->second.slo.shares != slo.shares) {
            sl_it->second.sg.set_shares(slo.shares);
            f = f.then([pc = sl_it->second.pc, shares = slo.shares] () {
                return engine().update_shares_for_class(pc, shares);
            });
        }

        sl_it->second.slo = slo;
    }
    return f;
}

future<> service_level_controller::notify_service_level_removed(sstring name) {
    auto sl_it = _service_levels_db.find(name);
    if (sl_it != _service_levels_db.end()) {
        unsigned sl_idx = internal::scheduling_group_index(sl_it->second.sg);
        _sl_lookup[sl_idx].first = nullptr;
        _sl_lookup[sl_idx].second = nullptr;
        if (this_shard_id() == global_controller) {
            _global_controller_db->deleted_scheduling_groups.emplace(sl_it->second.sg);
            _global_controller_db->deleted_priority_classes.emplace(sl_it->second.pc);
        }
        _service_levels_db.erase(sl_it);
    }
    return make_ready_future();
}

io_priority_class* service_level_controller::get_current_priority_class() {
    unsigned sched_idx = internal::scheduling_group_index(current_scheduling_group());
    if (_sl_lookup[sched_idx].second) {
        return &(_sl_lookup[sched_idx].second->pc);
    } else {
        return nullptr;
    }
}

scheduling_group service_level_controller::get_default_scheduling_group() {
    return _default_service_level.sg;
}

scheduling_group service_level_controller::get_scheduling_group(sstring service_level_name) {
    auto service_level_it = _service_levels_db.find(service_level_name);
    if (service_level_it != _service_levels_db.end()) {
        return service_level_it->second.sg;
    } else {
        return get_default_scheduling_group();
    }
}

bool service_level_controller::is_default_service_level() {
    return (current_scheduling_group() == get_default_scheduling_group());
}

std::optional<sstring> service_level_controller::get_active_service_level() {
    unsigned sched_idx = internal::scheduling_group_index(current_scheduling_group());
    if (_sl_lookup[sched_idx].first) {
        return sstring(*_sl_lookup[sched_idx].first);
    } else {
        return std::nullopt;
    }
}

void service_level_controller::update_from_distributed_data(std::chrono::duration<float> interval) {
    // FIXME: ignored future
    (void)container().invoke_on(global_controller, [interval] (service_level_controller& global_sl) {
        if (global_sl._global_controller_db->distributed_data_update.available()) {
            sl_logger.info("update_from_distributed_data: starting configuration polling loop");
            global_sl._global_controller_db->distributed_data_update = repeat([interval, &global_sl] {
                return sleep_abortable<steady_clock_type>(std::chrono::duration_cast<steady_clock_type::duration>(interval),
                        global_sl._global_controller_db->dist_data_update_aborter).then_wrapped([&global_sl] (future<>&& f) {
                    try {
                        f.get();
                        return global_sl.update_service_levels_from_distributed_data().then_wrapped([] (future<>&& f){
                            try {
                                f.get();
                            } catch (...) {
                                sl_logger.warn("update_from_distributed_data: exception occurred in distributed"
                                        " data check loop: {}", std::current_exception());
                            }
                            return stop_iteration::no;
                        });
                    } catch (const sleep_aborted& e) {
                        sl_logger.info("update_from_distributed_data: configuration polling loop aborted");
                        return make_ready_future<seastar::bool_class<seastar::stop_iteration_tag>>(stop_iteration::yes);
                    }
                });
            }).then_wrapped([] (future<>&& f) {
                try {
                    f.get();
                } catch (...) {
                    sl_logger.error("update_from_distributed_data: polling loop stopped unexpectedly by: {}",
                            std::current_exception());
                }
            });
        }
    });
}

future<> service_level_controller::add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exists) {
    set_service_level_op_type add_type = if_not_exists ? set_service_level_op_type::add_if_not_exists : set_service_level_op_type::add;
    return set_distributed_service_level(name, slo, add_type);
}

future<> service_level_controller::alter_distributed_service_level(sstring name, service_level_options slo) {
    return set_distributed_service_level(name, slo, set_service_level_op_type::alter);
}

future<> service_level_controller::drop_distributed_service_level(sstring name, bool if_exists) {
    return _sl_data_accessor->get_service_levels().then([this, name, if_exists] (qos::service_levels_info sl_info) {
        auto it = sl_info.find(name);
        if (it == sl_info.end()) {
            if (if_exists) {
                return make_ready_future();
            } else {
                return make_exception_future(nonexistant_service_level_exception(name));
            }
        } else {
            auto& role_manager = _auth_service.local().underlying_role_manager();
            return role_manager.query_attribute_for_all("service_level").then( [&role_manager, name] (auth::role_manager::attribute_vals attributes) {
                return parallel_for_each(attributes.begin(), attributes.end(), [&role_manager, name] (auto&& attr) {
                    if (attr.second == name) {
                        return role_manager.remove_attribute(attr.first, "service_level");
                    } else {
                        return make_ready_future();
                    }
                });
            }).then([this, name] {
                return _sl_data_accessor->drop_service_level(name);
            });
        }
    });
}

future<service_levels_info> service_level_controller::get_distributed_service_levels() {
    return _sl_data_accessor->get_service_levels();
}

future<service_levels_info> service_level_controller::get_distributed_service_level(sstring service_level_name) {
    return _sl_data_accessor->get_service_level(service_level_name);
}

future<> service_level_controller::set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type) {
    return _sl_data_accessor->get_service_levels().then([this, name, slo, op_type] (qos::service_levels_info sl_info) {
        auto it = sl_info.find(name);
        // test for illegal requests or requests that should terminate without any action
        if (it == sl_info.end()) {
            if (op_type == set_service_level_op_type::alter) {
                return make_exception_future(exceptions::invalid_request_exception(format("The service level '{}' desn't exist.", name)));
            }
        } else {
            if (op_type == set_service_level_op_type::add) {
                return make_exception_future(exceptions::invalid_request_exception(format("The service level '{}' already exists.", name)));
            } else if (op_type == set_service_level_op_type::add_if_not_exists) {
                return make_ready_future();
            }
        }
        return _sl_data_accessor->set_service_level(name, slo);
    });
}

future<> service_level_controller::do_add_service_level(sstring name, service_level_options slo, bool is_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (is_static) {
        _global_controller_db->static_configurations[name] = slo;
    }
    if (service_level_it != _service_levels_db.end()) {
        if ((is_static && service_level_it->second.is_static) || !is_static) {
           if ((service_level_it->second.is_static) && (!is_static)) {
               service_level_it->second.is_static = false;
           }
           return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, slo);
        } else {
            // this means we set static layer when the the service level
            // is running of the non static configuration. so we have nothing
            // else to do since we already saved the static configuration.
            return make_ready_future();
        }
    } else {
        return do_with(service_level{slo /*slo*/, default_scheduling_group() /*sg*/, default_priority_class() /*pc*/,
                false /*marked_for_deletion*/, is_static /*is_static*/}, std::move(name), [this] (service_level& sl, sstring& name) {
            return make_ready_future().then([this, &sl] () mutable {
                if (!_global_controller_db->deleted_scheduling_groups.empty()) {
                    scheduling_group sg;
                    sg =  _global_controller_db->deleted_scheduling_groups.top();
                    _global_controller_db->deleted_scheduling_groups.pop();
                    sl.sg = sg;
                    return container().invoke_on_all([sg, &sl] (service_level_controller& service) {
                        scheduling_group non_const_sg = sg;
                        return non_const_sg.set_shares((float)sl.slo.shares);
                    });
                } else {
                   return create_scheduling_group(format("service_level_sg_{}", _global_controller_db->schedg_group_cnt++), sl.slo.shares).then([&sl] (scheduling_group sg) {
                       sl.sg = sg;
                   });
                }
            }).then([this, &sl] () mutable {
                if (!_global_controller_db->deleted_priority_classes.empty()) {
                    io_priority_class pc = _global_controller_db->deleted_priority_classes.top();
                    _global_controller_db->deleted_priority_classes.pop();
                    sl.pc = pc;
                    return container().invoke_on_all([pc, sl] (service_level_controller& service) {
                        return engine().update_shares_for_class(pc, sl.slo.shares);
                    });
                } else {
                    sl. pc = engine().
                            register_one_priority_class(format("service_level_pc_{}", _global_controller_db->io_priority_cnt++), sl.slo.shares);
                    return make_ready_future();
                }
            }).then([this, &sl, name] () {
                return container().invoke_on_all(&service_level_controller::notify_service_level_added, name, sl);
            });
        });
    }
    return make_ready_future();
}

future<> service_level_controller::do_remove_service_level(sstring name, bool remove_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (service_level_it != _service_levels_db.end()) {
        auto static_conf_it = _global_controller_db->static_configurations.end();
        bool static_exists = false;
        if (remove_static) {
            _global_controller_db->static_configurations.erase(name);
        } else {
            static_conf_it = _global_controller_db->static_configurations.find(name);
            static_exists = static_conf_it != _global_controller_db->static_configurations.end();
        }
        if (remove_static && service_level_it->second.is_static) {
            return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
        } else if (!remove_static && !service_level_it->second.is_static) {
            if (static_exists) {
                service_level_it->second.is_static = true;
                return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, static_conf_it->second);
            } else {
                return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
            }
        }
    }
    return make_ready_future();
}

}
