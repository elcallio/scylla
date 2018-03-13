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

#include "cql3/statements/schema_altering_statement.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

schema_altering_statement::schema_altering_statement()
    : cf_statement{::shared_ptr<cf_name>{}}
    , _is_column_family_level{false}
{
}

schema_altering_statement::schema_altering_statement(::shared_ptr<cf_name> name)
    : cf_statement{std::move(name)}
    , _is_column_family_level{true}
{
}

bool schema_altering_statement::uses_function(const sstring& ks_name, const sstring& function_name) const
{
    return cf_statement::uses_function(ks_name, function_name);
}

bool schema_altering_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool schema_altering_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

uint32_t schema_altering_statement::get_bound_terms()
{
    return 0;
}

void schema_altering_statement::prepare_keyspace(const service::client_state& state)
{
    if (_is_column_family_level) {
        cf_statement::prepare_keyspace(state);
    }
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute0(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options, bool is_local_only) {
    // If an IF [NOT] EXISTS clause was used, this may not result in an actual schema change.  To avoid doing
    // extra work in the drivers to handle schema changes, we return an empty message in this case. (CASSANDRA-7600)
    return announce_migration(proxy, is_local_only).then([this] (auto ce) {
        ::shared_ptr<messages::result_message> result;
        if (!ce) {
            result = ::make_shared<messages::result_message::void_message>();
        } else {
            result = ::make_shared<messages::result_message::schema_change>(ce);
        }
        return make_ready_future<::shared_ptr<messages::result_message>>(result);
    });
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    return execute0(proxy, state, options, false);
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    return execute0(proxy, state, options, true);
}

audit::statement_category schema_altering_statement::category() const {
    return audit::statement_category::DDL;
}

}

}
