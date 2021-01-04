/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/seastar.hh>
#include "init.hh"
#include "supervisor.hh"
#include "directories.hh"
#include "distributed_loader.hh"
#include "utils/disk-error-handler.hh"
#include "db/config.hh"

namespace utils {

static future<> disk_sanity(fs::path path, bool developer_mode) {
    return check_direct_io_support(path.native()).then([] {
        return make_ready_future<>();
    }).handle_exception([path](auto ep) {
        startlog.error("Could not access {}: {}", path, ep);
        return make_exception_future<>(ep);
    });
};

static future<file_lock> touch_and_lock(fs::path path) {
    return io_check([path] { return recursive_touch_directory(path.native()); }).then_wrapped([path] (future<> f) {
        try {
            f.get();
            return file_lock::acquire(path / ".lock").then_wrapped([path](future<file_lock> f) {
                // only do this because "normal" unhandled exception exit in seastar
                // _drops_ system_error message ("what()") and thus does not quite deliver
                // the relevant info to the user
                try {
                    return make_ready_future<file_lock>(f.get());
                } catch (std::exception& e) {
                    startlog.error("Could not initialize {}: {}", path, e.what());
                    throw;
                } catch (...) {
                    throw;
                }
            });
        } catch (...) {
            startlog.error("Directory '{}' cannot be initialized. Tried to do it but failed with: {}", path, std::current_exception());
            throw;
        }
    });
}

void directories::set::add(fs::path path) {
    _paths.insert(path);
}

void directories::set::add(sstring path) {
    add(fs::path(path));
}

void directories::set::add(std::vector<sstring> paths) {
    for (auto& path : paths) {
        add(path);
    }
}

void directories::set::add_sharded(sstring p) {
    fs::path path(p);

    for (unsigned i = 0; i < smp::count; i++) {
        add(path / seastar::to_sstring(i).c_str());
    }
}

directories::directories(bool developer_mode)
        : _developer_mode(developer_mode)
{ }

future<> directories::create_and_verify(directories::set dir_set) {
    return do_with(std::vector<file_lock>(), [this, dir_set = std::move(dir_set)] (std::vector<file_lock>& locks) {
        return parallel_for_each(dir_set.get_paths(), [this, &locks] (fs::path path) {
            return touch_and_lock(path).then([path = std::move(path), developer_mode = _developer_mode, &locks] (file_lock lock) {
                locks.emplace_back(std::move(lock));
                return disk_sanity(path, developer_mode).then([path = std::move(path)] {
                    return distributed_loader::verify_owner_and_mode(path).handle_exception([](auto ep) {
                        startlog.error("Failed owner and mode verification: {}", ep);
                        return make_exception_future<>(ep);
                    });
                });
            });
        }).then([this, &locks] {
            std::move(locks.begin(), locks.end(), std::back_inserter(_locks));
        });
    });
}

} // namespace utils
