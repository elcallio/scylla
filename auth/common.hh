/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <chrono>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/smp.hh>

#include "log.hh"
#include "seastarx.hh"
#include "utils/exponential_backoff_retry.hh"

using namespace std::chrono_literals;

class database;
class timeout_config;

class database;

namespace service {
class migration_manager;
}

namespace cql3 {
class query_processor;
}

namespace auth {

namespace meta {

constexpr std::string_view DEFAULT_SUPERUSER_NAME("cassandra");
extern const std::string_view AUTH_KS;
extern const std::string_view USERS_CF;
extern const std::string_view AUTH_PACKAGE_NAME;

}

template <class Task>
future<> once_among_shards(Task&& f) {
    if (this_shard_id() == 0u) {
        return f();
    }

    return make_ready_future<>();
}

inline future<> delay_until_system_ready(seastar::abort_source& as) {
    return sleep_abortable(15s, as);
}

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func);

future<> create_metadata_table_if_missing(
        std::string_view table_name,
        cql3::query_processor&,
        std::string_view cql,
        ::service::migration_manager&) noexcept;

future<> wait_for_schema_agreement(::service::migration_manager&, const database&, seastar::abort_source&);

///
/// Time-outs for internal, non-local CQL queries.
///
const timeout_config& internal_distributed_timeout_config() noexcept;

}
