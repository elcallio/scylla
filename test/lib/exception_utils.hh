/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <experimental/source_location>
#include <functional>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace exception_predicate {

/// Makes an exception predicate that applies \p check function to verify the exception and \p err
/// function to create an error message if the check fails.
extern std::function<bool(const std::exception&)> make(
        std::function<bool(const std::exception&)> check,
        std::function<sstring(const std::exception&)> err);

/// Returns a predicate that will check if the exception message contains the given fragment.
extern std::function<bool(const std::exception&)> message_contains(
        const sstring& fragment,
        const std::experimental::source_location& loc = std::experimental::source_location::current());

/// Returns a predicate that will check if the exception message equals the given text.
extern std::function<bool(const std::exception&)> message_equals(
        const sstring& text,
        const std::experimental::source_location& loc = std::experimental::source_location::current());

} // namespace exception_predicate
