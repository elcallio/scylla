/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <utility>
#include <optional>
#include "dht/token.hh"
#include "seastarx.hh"

namespace dht {

class token;

}

namespace db::system_keyspace {

using view_name = std::pair<sstring, sstring>;

struct view_build_progress {
    view_name view;
    dht::token first_token;
    std::optional<dht::token> next_token;
    shard_id cpu_id;
};

}
