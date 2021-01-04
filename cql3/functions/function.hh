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
 * Copyright (C) 2014 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "function_name.hh"
#include "types.hh"
#include <vector>
#include <optional>

namespace cql3 {
namespace functions {

class function {
public:
    using opt_bytes = std::optional<bytes>;
    virtual ~function() {}
    virtual const function_name& name() const = 0;
    virtual const std::vector<data_type>& arg_types() const = 0;
    virtual const data_type& return_type() const = 0;

    /**
     * Checks whether the function is a pure function (as in doesn't depend on, nor produce side effects) or not.
     *
     * @return <code>true</code> if the function is a pure function, <code>false</code> otherwise.
     */
    virtual bool is_pure() const = 0;

    /**
     * Checks whether the function is a native/hard coded one or not.
     *
     * @return <code>true</code> if the function is a native/hard coded one, <code>false</code> otherwise.
     */
    virtual bool is_native() const = 0;

    virtual bool requires_thread() const = 0;

    /**
     * Checks whether the function is an aggregate function or not.
     *
     * @return <code>true</code> if the function is an aggregate function, <code>false</code> otherwise.
     */
    virtual bool is_aggregate() const = 0;

    virtual void print(std::ostream& os) const = 0;

    /**
     * Returns the name of the function to use within a ResultSet.
     *
     * @param column_names the names of the columns used to call the function
     * @return the name of the function to use within a ResultSet
     */
    virtual sstring column_name(const std::vector<sstring>& column_names) const = 0;

    friend class function_call;
    friend std::ostream& operator<<(std::ostream& os, const function& f);
};

inline
std::ostream&
operator<<(std::ostream& os, const function& f) {
    f.print(os);
    return os;
}

}
}
