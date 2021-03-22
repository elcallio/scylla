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

#include "cql3/index_name.hh"

namespace cql3 {

void index_name::set_index(const sstring& idx, bool keep_case)
{
    _idx_name = to_internal_name(idx, keep_case);
}

const sstring& index_name::get_idx() const
{
    return _idx_name;
}

cf_name index_name::get_cf_name() const
{
    cf_name cf;
    if (has_keyspace()) {
        cf.set_keyspace(get_keyspace(), true);
    }
    return cf;
}

sstring index_name::to_string() const
{
    return keyspace_element_name::to_string() + _idx_name;
}

}
