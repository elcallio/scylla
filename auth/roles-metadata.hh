/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string_view>
#include <functional>

#include <seastar/core/future.hh>

#include "seastarx.hh"

namespace cql3 {
class query_processor;
class untyped_result_set_row;
}

namespace auth {

namespace meta {

namespace roles_table {

std::string_view creation_query();

constexpr std::string_view name{"roles", 5};

extern const std::string_view qualified_name;

constexpr std::string_view role_col_name{"role", 4};

}

}

///
/// Check that the default role satisfies a predicate, or `false` if the default role does not exist.
///
future<bool> default_role_row_satisfies(
        cql3::query_processor&,
        std::function<bool(const cql3::untyped_result_set_row&)>);

///
/// Check that any nondefault role satisfies a predicate. `false` if no nondefault roles exist.
///
future<bool> any_nondefault_role_row_satisfies(
        cql3::query_processor&,
        std::function<bool(const cql3::untyped_result_set_row&)>);

}
