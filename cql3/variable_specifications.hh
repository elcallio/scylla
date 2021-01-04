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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"

#include <optional>
#include <vector>

namespace cql3 {

class variable_specifications final {
private:
    std::vector<shared_ptr<column_identifier>> _variable_names;
    std::vector<lw_shared_ptr<column_specification>> _specs;
    std::vector<lw_shared_ptr<column_specification>> _target_columns;

public:

    variable_specifications() = default;
    variable_specifications(const std::vector<::shared_ptr<column_identifier>>& variable_names);

    /**
     * Returns an empty instance of <code>VariableSpecifications</code>.
     * @return an empty instance of <code>VariableSpecifications</code>
     */
    static lw_shared_ptr<variable_specifications> empty();

    size_t size() const;

    std::vector<lw_shared_ptr<column_specification>> get_specifications() const &;

    std::vector<lw_shared_ptr<column_specification>> get_specifications() &&;

    std::vector<uint16_t> get_partition_key_bind_indexes(const schema& schema) const;

    void add(int32_t bind_index, lw_shared_ptr<column_specification> spec);

    void set_bound_variables(const std::vector<shared_ptr<column_identifier>>& bound_names);
};

}
