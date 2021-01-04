/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <set>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include "utils/file_lock.hh"

using namespace seastar;

namespace db {
class config;
}

namespace utils {

class directories {
public:
    class set {
    public:
        void add(fs::path path);
        void add(sstring path);
        void add(std::vector<sstring> path);
        void add_sharded(sstring path);

        const std::set<fs::path> get_paths() const {
            return _paths;
        }

    private:
        std::set<fs::path> _paths;
    };

    directories(bool developer_mode);
    future<> create_and_verify(set dir_set);
private:
    bool _developer_mode;
    std::vector<file_lock> _locks;
};

} // namespace utils
