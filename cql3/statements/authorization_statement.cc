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
 * Copyright 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "authorization_statement.hh"
#include "transport/messages/result_message.hh"

uint32_t cql3::statements::authorization_statement::get_bound_terms() const {
    return 0;
}

bool cql3::statements::authorization_statement::depends_on_keyspace(
                const sstring& ks_name) const {
    return false;
}

bool cql3::statements::authorization_statement::depends_on_column_family(
                const sstring& cf_name) const {
    return false;
}

void cql3::statements::authorization_statement::validate(
                service::storage_proxy&,
                const service::client_state& state) const {
}

future<> cql3::statements::authorization_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const {
    return make_ready_future<>();
}

void cql3::statements::authorization_statement::maybe_correct_resource(auth::resource& resource, const service::client_state& state){
    if (resource.kind() == auth::resource_kind::data) {
        const auto data_view = auth::data_resource_view(resource);
        const auto keyspace = data_view.keyspace();
        const auto table = data_view.table();

        if (table && keyspace->empty()) {
            resource = auth::make_data_resource(state.get_keyspace(), *table);
        }
    }
}

audit::statement_category cql3::statements::authorization_statement::category() const {
    return audit::statement_category::DCL;
}

