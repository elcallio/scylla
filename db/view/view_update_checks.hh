/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "db/system_distributed_keyspace.hh"
#include "streaming/stream_reason.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

namespace db::view {

future<bool> check_view_build_ongoing(db::system_distributed_keyspace& sys_dist_ks, const sstring& ks_name, const sstring& cf_name);
future<bool> check_needs_view_update_path(db::system_distributed_keyspace& sys_dist_ks, const table& t, streaming::stream_reason reason);

}
