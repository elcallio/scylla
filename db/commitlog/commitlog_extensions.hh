/*
 * Copyright 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <tuple>
#include <optional>
#include <seastar/core/seastar.hh>

#include "commitlog.hh"

namespace db {
    class commitlog_file_extension {
    public:
        virtual ~commitlog_file_extension() {}
        virtual future<file> wrap_file(const sstring& filename, file, open_flags flags) = 0;
        virtual future<> before_delete(const sstring& filename) = 0;
    };
}

