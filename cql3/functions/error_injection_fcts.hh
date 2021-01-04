/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "native_scalar_function.hh"

namespace cql3
{

namespace functions
{

namespace error_injection
{

class failure_injection_function  : public native_scalar_function {
protected:
    failure_injection_function(sstring name, data_type return_type, std::vector<data_type> args_type)
            : native_scalar_function(std::move(name), std::move(return_type), std::move(args_type)) {
    }

    bool requires_thread() const override {
        return true;
    }
};

shared_ptr<function> make_enable_injection_function();
shared_ptr<function> make_disable_injection_function();
shared_ptr<function> make_enabled_injections_function();

} // namespace error_injection

} // namespace functions

} // namespace cql3
