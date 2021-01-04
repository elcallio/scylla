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

#include "types.hh"

namespace cql3 {

class column_identifier;

class column_specification final {
public:
    const sstring ks_name;
    const sstring cf_name;
    const ::shared_ptr<column_identifier> name;
    const data_type type;

    column_specification(std::string_view ks_name_, std::string_view cf_name_, ::shared_ptr<column_identifier> name_, data_type type_);

    /**
     * Returns a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     *
     * @param alias the column alias
     * @return a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     */
    lw_shared_ptr<column_specification> with_alias(::shared_ptr<column_identifier> alias) {
        return make_lw_shared<column_specification>(ks_name, cf_name, alias, type);
    }
    
    bool is_reversed_type() const {
        return ::dynamic_pointer_cast<const reversed_type_impl>(type) != nullptr;
    }

    static bool all_in_same_table(const std::vector<lw_shared_ptr<column_specification>>& names);
};

}
