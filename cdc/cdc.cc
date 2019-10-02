/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <utility>

#include "cdc/cdc.hh"
#include "bytes.hh"
#include "database.hh"
#include "db/config.hh"
#include "dht/murmur3_partitioner.hh"
#include "partition_slice_builder.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"

using seastar::sstring;

namespace cdc {

using operation_native_type = std::underlying_type_t<operation>;
using column_op_native_type = std::underlying_type_t<column_op>;

sstring log_name(const sstring& table_name) {
    static constexpr const auto cdc_log_suffix = "_scylla_cdc_log";
    return table_name + cdc_log_suffix;
}

sstring desc_name(const sstring& table_name) {
    static constexpr const auto cdc_desc_suffix = "_scylla_cdc_desc";
    return table_name + cdc_desc_suffix;
}

static future<> remove_log(const sstring& ks_name, const sstring& table_name) {
    auto& mm = service::get_local_migration_manager();
    return mm.announce_column_family_drop(ks_name, log_name(table_name), false);
}

static future<> remove_desc(const sstring& ks_name, const sstring& table_name) {
    auto& mm = service::get_local_migration_manager();
    return mm.announce_column_family_drop(ks_name, desc_name(table_name), false);
}

future<> remove(const sstring& ks_name, const sstring& table_name) {
    return when_all(remove_log(ks_name, table_name), remove_desc(ks_name, table_name)).discard_result();
}

static future<> setup_log(const schema& s) {
    schema_builder b(s.ks_name(), log_name(s.cf_name()));
    b.set_default_time_to_live(gc_clock::duration{86400}); // 24h
    b.set_comment(sprint("CDC log for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("stream_id", uuid_type, column_kind::partition_key);
    b.with_column("time", timeuuid_type, column_kind::clustering_key);
    b.with_column("batch_seq_no", int32_type, column_kind::clustering_key);
    b.with_column("operation", data_type_for<operation_native_type>());
    b.with_column("ttl", long_type);
    auto add_columns = [&] (const schema::const_iterator_range_type& columns, bool is_data_col = false) {
        for (const auto& column : columns) {
            auto type = column.type;
            if (is_data_col) {
                type = tuple_type_impl::get_instance({ data_type_for<column_op_native_type>(), type, long_type});
        }
            b.with_column("_" + column.name(), type);
        }
    };
    add_columns(s.partition_key_columns());
    add_columns(s.clustering_key_columns());
    add_columns(s.static_columns(), true);
    add_columns(s.regular_columns(), true);
    auto& mm = service::get_local_migration_manager();
    return mm.announce_new_column_family(b.build(), false);
}

static future<> setup_desc(const schema& s) {
    schema_builder b(s.ks_name(), desc_name(s.cf_name()));
    b.set_comment(sprint("CDC description for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("node_ip", inet_addr_type, column_kind::partition_key);
    b.with_column("shard_id", int32_type, column_kind::partition_key);
    b.with_column("created_at", timestamp_type, column_kind::clustering_key);
    b.with_column("stream_id", uuid_type);
    auto& mm = service::get_local_migration_manager();
    return mm.announce_new_column_family(b.build(), false);
}

template <typename Func>
static void for_each_endpoint(Func&& f) {
    auto& topology = service::get_local_storage_service().get_token_metadata().get_topology();
    for (const auto& dc : topology.get_datacenter_endpoints()) {
        for (const auto& endpoint : dc.second) {
            f(endpoint);
        }
    }
}

template <typename Func>
static void for_each_shard(const gms::inet_address& endpoint, Func&& f) {
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    const unsigned int shard_count = snitch_ptr->get_shard_count(endpoint);
    for (unsigned int shard_id = 0; shard_id < shard_count; ++shard_id) {
        f(shard_id, shard_count);
    }
}

static future<>
populate_desc(service::storage_proxy& proxy, const schema& s) {
    auto& db = proxy.get_db().local();
    auto desc_schema =
        db.find_schema(s.ks_name(), desc_name(s.cf_name()));
    auto log_schema =
        db.find_schema(s.ks_name(), log_name(s.cf_name()));
    auto& token_metadata =
        service::get_local_storage_service().get_token_metadata();
    auto& replication_strategy =
        db.find_keyspace(s.ks_name()).get_replication_strategy();
    // The assumption here is that all nodes in the cluster are using the
    // same murmur3_partitioner_ignore_msb_bits. If it's not true, we will
    // have to gossip it around together with shard_count.
    auto ignore_msb_bits =
        db.get_config().murmur3_partitioner_ignore_msb_bits();

    auto belongs_to = [&](const gms::inet_address& endpoint,
                          const unsigned int shard_id,
                          const int shard_count,
                          const utils::UUID& stream_id) {
        const auto log_pk = partition_key::from_singular(*log_schema,
                                                         data_value(stream_id));
        const auto token =
            dht::global_partitioner().decorate_key(*log_schema, log_pk).token();
        auto&& eps =
            replication_strategy.calculate_natural_endpoints(token, token_metadata);
        if (eps.empty() || eps[0] != endpoint) {
            return false;
        }
        const auto owning_shard_id = dht::murmur3_partitioner::shard_of(
                token, shard_count, ignore_msb_bits);
        return owning_shard_id == shard_id;
    };

    std::vector<mutation> mutations;
    const auto ts = api::new_timestamp();
    const auto ck = clustering_key::from_single_value(
            *desc_schema, timestamp_type->decompose(ts));
    auto cdef = desc_schema->get_column_definition(to_bytes("stream_id"));

    for_each_endpoint([&] (const gms::inet_address& endpoint) {
        const auto decomposed_ip = inet_addr_type->decompose(endpoint.addr());

        for_each_shard(endpoint, [&] (unsigned int shard_id, int shard_count) {
            const auto pk = partition_key::from_exploded(
                    *desc_schema, { decomposed_ip, int32_type->decompose(static_cast<int>(shard_id)) });
            mutations.emplace_back(desc_schema, pk);

            auto stream_id = utils::make_random_uuid();
            while (!belongs_to(endpoint, shard_id, shard_count, stream_id)) {
                stream_id = utils::make_random_uuid();
            }
            auto value = atomic_cell::make_live(*uuid_type,
                                                ts,
                                                uuid_type->decompose(stream_id));
            mutations.back().set_cell(ck, *cdef, std::move(value));
        });
    });
    return proxy.mutate(std::move(mutations),
                        db::consistency_level::QUORUM,
                        db::no_timeout,
                        nullptr,
                        empty_service_permit());
}

future<> setup(service::storage_proxy& proxy, schema_ptr s) {
    return setup_log(*s).then([&proxy, s = std::move(s)] () mutable {
        return setup_desc(*s).then_wrapped([&proxy, s = std::move(s)] (future<> f) mutable {
            if (f.failed()) {
                return remove_log(s->ks_name(), s->cf_name());
            }
            return populate_desc(proxy, *s).handle_exception([s = std::move(s)] (std::exception_ptr ep) mutable {
                return remove_desc(s->ks_name(), s->cf_name()).then([s = std::move(s)] {
                    return remove_log(s->ks_name(), s->cf_name());
                }).then([ep = std::move(ep)] {
                    return make_exception_future<>(std::move(ep));
                });;
            });
        });
    });
}

class transformer final {
public:
    struct map_cmp {
        bool operator()(const std::pair<net::inet_address, unsigned int>& a,
                        const std::pair<net::inet_address, unsigned int>& b) const {
            if (a.first.is_ipv4()) {
                if (b.first.is_ipv4()) {
                    if (a.first.as_ipv4_address().ip > b.first.as_ipv4_address().ip) {
                        return false;
                    }
                    return a.second < b.second;
                } else {
                    return true;
                }
            } else {
                if (b.first.is_ipv4()) {
                    return false;
                } else {
                    if (a.first.as_ipv6_address().ip > b.first.as_ipv6_address().ip) {
                        return false;
                    }
                    return a.second < b.second;
                }
            }
        }
    };
    using streams_type = std::map<std::pair<net::inet_address, unsigned int>, utils::UUID, map_cmp>;
private:
    schema_ptr _schema;
    schema_ptr _log_schema;
    utils::UUID _time;
    bytes _decomposed_time;
    std::reference_wrapper<locator::token_metadata> _token_metadata;
    std::reference_wrapper<const locator::abstract_replication_strategy> _replication_strategy;
    // The assumption here is that all nodes in the cluster are using the
    // same murmur3_partitioner_ignore_msb_bits. If it's not true, we will
    // have to gossip it around together with shard_count.
    unsigned _ignore_msb_bits;
    streams_type _streams;

    clustering_key set_pk_columns(const partition_key& pk, int batch_no, mutation& m) const {
        const auto log_ck = clustering_key::from_exploded(
                *m.schema(), { _decomposed_time, int32_type->decompose(batch_no) });
        auto pk_value = pk.explode(*_schema);
        size_t pos = 0;
        for (const auto& column : _schema->partition_key_columns()) {
            assert (pos < pk_value.size());
            auto cdef = m.schema()->get_column_definition(to_bytes("_" + column.name()));
            auto value = atomic_cell::make_live(*column.type,
                                                _time.timestamp(),
                                                bytes_view(pk_value[pos]));
            m.set_cell(log_ck, *cdef, std::move(value));
            ++pos;
        }
        return log_ck;
    }
    partition_key stream_id(const schema& s, const net::inet_address& ip, unsigned int shard_id) const {
        auto it = _streams.find(std::make_pair(ip, shard_id));
        if (it == std::end(_streams)) {
                throw std::runtime_error(format("No stream found for node {} and shard {}", ip, shard_id));
        }
        return partition_key::from_exploded(s, { uuid_type->decompose(it->second) });
    }
public:
    transformer(database& db, schema_ptr s, streams_type streams)
        : _schema(std::move(s))
        , _log_schema(db.find_schema(_schema->ks_name(), log_name(_schema->cf_name())))
        , _time(utils::UUID_gen::get_time_UUID())
        , _decomposed_time(timeuuid_type->decompose(_time))
        , _token_metadata(service::get_local_storage_service().get_token_metadata())
        , _replication_strategy(db.find_keyspace(_schema->ks_name()).get_replication_strategy())
        , _ignore_msb_bits(db.get_config().murmur3_partitioner_ignore_msb_bits())
        , _streams(std::move(streams))
    { }

    mutation transform(const mutation& m) const {
        auto& t = m.token();
        auto&& eps = _replication_strategy.get().calculate_natural_endpoints(t, _token_metadata.get());
        if (eps.empty()) {
            throw std::runtime_error(format("No primary replica found for key {}", m.decorated_key()));
        }
        auto shard_id = dht::murmur3_partitioner::shard_of(
                t, locator::i_endpoint_snitch::get_local_snitch_ptr()->get_shard_count(eps[0]), _ignore_msb_bits);
        mutation res(_log_schema, stream_id(*_log_schema, eps[0].addr(), shard_id));
        auto& p = m.partition();
        if(p.partition_tombstone()) {
            // Partition deletion
            set_pk_columns(m.key(), 0, res);
        } else if (!p.row_tombstones().empty()) {
            // range deletion
            int batch_no = 0;
            for (auto& rt : p.row_tombstones()) {
                auto set_bound = [&] (const clustering_key& log_ck, const clustering_key_prefix& ckp) {
                    auto exploded = ckp.explode(*_schema);
                    size_t pos = 0;
                    for (const auto& column : _schema->clustering_key_columns()) {
                        if (pos >= exploded.size()) {
                            break;
                        }
                        auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
                        auto value = atomic_cell::make_live(*column.type,
                                                            _time.timestamp(),
                                                            bytes_view(exploded[pos]));
                        res.set_cell(log_ck, *cdef, std::move(value));
                        ++pos;
                    }
                };
                {
                    auto log_ck = set_pk_columns(m.key(), batch_no, res);
                    set_bound(log_ck, rt.start);
                    ++batch_no;
                }
                {
                    auto log_ck = set_pk_columns(m.key(), batch_no, res);
                    set_bound(log_ck, rt.end);
                    ++batch_no;
                }
            }
        } else {
            // should be update or deletion
            int batch_no = 0;
            for (const rows_entry& r : p.clustered_rows()) {
                auto log_ck = set_pk_columns(m.key(), batch_no, res);
                auto ck_value = r.key().explode(*_schema);
                size_t pos = 0;
                for (const auto& column : _schema->clustering_key_columns()) {
                    assert (pos < ck_value.size());
                    auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
                    auto value = atomic_cell::make_live(*column.type,
                                                        _time.timestamp(),
                                                        bytes_view(ck_value[pos]));
                    res.set_cell(log_ck, *cdef, std::move(value));
                    ++pos;
                }

                ++batch_no;
            }
        }
        return res;
    }
};

class streams_builder {
    schema_ptr _schema;
    transformer::streams_type _streams{transformer::map_cmp{}};
    net::inet_address _node_ip = net::inet_address();
    unsigned int _shard_id = 0;
    api::timestamp_type _latest_row_timestamp = api::min_timestamp;
    utils::UUID _latest_row_stream_id = utils::UUID();
public:
    streams_builder(schema_ptr s) : _schema(std::move(s)) {}

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        auto exploded = key.explode(*_schema);
        _node_ip = value_cast<net::inet_address>(inet_addr_type->deserialize(exploded[0]));
        _shard_id = static_cast<unsigned int>(value_cast<int>(int32_type->deserialize(exploded[1])));
        _latest_row_timestamp = api::min_timestamp;
        _latest_row_stream_id = utils::UUID();
    }

    void accept_new_partition(uint32_t row_count) {
        assert(false);
    }

    void accept_new_row(
            const clustering_key& key,
            const query::result_row_view& static_row,
            const query::result_row_view& row) {
        auto row_iterator = row.iterator();
        api::timestamp_type timestamp = value_cast<db_clock::time_point>(
                timestamp_type->deserialize(key.explode(*_schema)[0])).time_since_epoch().count();
        if (timestamp <= _latest_row_timestamp) {
            return;
        }
        _latest_row_timestamp = timestamp;
        for (auto&& cdef : _schema->regular_columns()) {
            if (cdef.name_as_text() != "stream_id") {
                row_iterator.skip(cdef);
                continue;
            }
            auto val_opt = row_iterator.next_atomic_cell();
            assert(val_opt);
            val_opt->value().with_linearized([&] (bytes_view bv) {
                _latest_row_stream_id = value_cast<utils::UUID>(uuid_type->deserialize(bv));
            });
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        assert(false);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        _streams.emplace(std::make_pair(_node_ip, _shard_id), _latest_row_stream_id);
    }

    transformer::streams_type build() {
        return std::move(_streams);
    }
};

static future<transformer> get_transformer(
        service::storage_proxy& proxy,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service_permit permit) {
    auto desc_schema =
        proxy.get_db().local().find_schema(s->ks_name(), desc_name(s->cf_name()));
    query::read_command cmd(
            desc_schema->id(),
            desc_schema->version(),
            partition_slice_builder(*desc_schema).with_no_static_columns().build());
    return proxy.query(
            desc_schema,
            make_lw_shared(std::move(cmd)),
            {dht::partition_range::make_open_ended_both_sides()},
            db::consistency_level::QUORUM,
            {timeout, permit}).then([&proxy, desc_schema = std::move(desc_schema), s = std::move(s)] (auto qr) mutable {
        return query::result_view::do_with(*qr.query_result, [&proxy, desc_schema = std::move(desc_schema), s = std::move(s)] (query::result_view v) mutable {
            auto slice = partition_slice_builder(*desc_schema).with_no_static_columns().build();
            streams_builder builder{ std::move(desc_schema) };
            v.consume(slice, builder);
            return transformer(proxy.get_db().local(), std::move(s), builder.build());
        });
    });
}

future<std::vector<mutation>> apply(
        service::storage_proxy& proxy,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service_permit permit,
        std::vector<mutation> mutations) {

    return get_transformer(proxy, std::move(s), timeout, permit).then([mutations = std::move(mutations)] (transformer trans) mutable {
        mutations.reserve(2 * mutations.size());
        for(int i = 0, size = mutations.size(); i < size; ++i) {
            mutations.push_back(trans.transform(mutations[i]));
        }
        return std::move(mutations);
    });
}

} // namespace cdc
