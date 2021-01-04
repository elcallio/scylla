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
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/column_specification.hh"

namespace cql3 {

column_specification::column_specification(std::string_view ks_name_, std::string_view cf_name_, ::shared_ptr<column_identifier> name_, data_type type_)
        : ks_name(ks_name_)
        , cf_name(cf_name_)
        , name(name_)
        , type(type_)
    { }


bool column_specification::all_in_same_table(const std::vector<lw_shared_ptr<column_specification>>& names)
{
    assert(!names.empty());

    auto first = names.front();
    return std::all_of(std::next(names.begin()), names.end(), [first] (auto&& spec) {
        return spec->ks_name == first->ks_name && spec->cf_name == first->cf_name;
    });
}

}
