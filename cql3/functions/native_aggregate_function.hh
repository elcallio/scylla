/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "types.hh"
#include "native_function.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {
namespace functions {

/**
 * Base class for the <code>AggregateFunction</code> native classes.
 */
class native_aggregate_function : public native_function, public aggregate_function {
protected:
    native_aggregate_function(sstring name, data_type return_type,
            std::vector<data_type> arg_types)
        : native_function(std::move(name), std::move(return_type), std::move(arg_types)) {
    }

public:
    virtual bool is_aggregate() const override final {
        return true;
    }
};

}
}
