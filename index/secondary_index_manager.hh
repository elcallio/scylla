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
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "schema.hh"

#include "cql3/expr/expression.hh"
#include "database_fwd.hh"

#include <vector>
#include <set>

namespace secondary_index {

sstring index_table_name(const sstring& index_name);

/*!
 * \brief a reverse of index_table_name
 * It gets a table_name and return the index name that was used
 * to create that table.
 */
sstring index_name_from_table_name(const sstring& table_name);

class index {
    sstring _target_column;
    index_metadata _im;
public:
    index(const sstring& target_column, const index_metadata& im);
    bool depends_on(const column_definition& cdef) const;
    bool supports_expression(const column_definition& cdef, const cql3::expr::oper_t op) const;
    const index_metadata& metadata() const;
    const sstring& target_column() const {
        return _target_column;
    }
};

class secondary_index_manager {
    column_family& _cf;
    /// The key of the map is the name of the index as stored in system tables.
    std::unordered_map<sstring, index> _indices;
public:
    secondary_index_manager(column_family& cf);
    void reload();
    view_ptr create_view_for_index(const index_metadata& index, bool new_token_column_computation) const;
    std::vector<index_metadata> get_dependent_indices(const column_definition& cdef) const;
    std::vector<index> list_indexes() const;
    bool is_index(view_ptr) const;
    bool is_index(const schema& s) const;
    bool is_global_index(const schema& s) const;
private:
    void add_index(const index_metadata& im);
};

}
