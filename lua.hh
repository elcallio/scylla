/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "types.hh"
#include "utils/updateable_value.hh"
#include "db/config.hh"
#include <seastar/core/future.hh>

namespace lua {
// type safe alias
struct bitcode_view {
    std::string_view bitcode;
};

struct runtime_config {
    utils::updateable_value<unsigned> timeout_in_ms;
    utils::updateable_value<unsigned> max_bytes;
    utils::updateable_value<unsigned> max_contiguous;
};

runtime_config make_runtime_config(const db::config& config);

sstring compile(const runtime_config& cfg, const std::vector<sstring>& arg_names, sstring script);
seastar::future<bytes_opt> run_script(bitcode_view bitcode, const std::vector<data_value>& values,
                                      data_type return_type, const runtime_config& cfg);
}
