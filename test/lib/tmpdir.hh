/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <fmt/format.h>

#include <seastar/util/std-compat.hh>

#include "utils/UUID.hh"

namespace fs = std::filesystem;

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
class tmpdir {
    fs::path _path;

private:
    void remove() noexcept;

public:
    tmpdir();

    tmpdir(tmpdir&& other) noexcept;
    tmpdir(const tmpdir&) = delete;
    void operator=(tmpdir&& other) noexcept;
    void operator=(const tmpdir&) = delete;

    ~tmpdir();

    const fs::path& path() const noexcept { return _path; }
};
