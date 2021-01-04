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

#include <seastar/core/sstring.hh>
#include "database_fwd.hh"
#include "schema_fwd.hh"

using namespace seastar;

namespace validation {

constexpr size_t max_key_size = std::numeric_limits<uint16_t>::max();

// Returns an error string if key is invalid, a disengaged optional otherwise.
std::optional<sstring> is_cql_key_invalid(const schema& schema, partition_key_view key);
void validate_cql_key(const schema& schema, partition_key_view key);
schema_ptr validate_column_family(const database& db, const sstring& keyspace_name, const sstring& cf_name);
void validate_keyspace(const database& db, const sstring& keyspace_name);

}
