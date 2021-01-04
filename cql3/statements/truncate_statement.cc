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

#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "database.hh"

#include <optional>

namespace cql3 {

namespace statements {

truncate_statement::truncate_statement(::shared_ptr<cf_name> name)
    : cf_statement{std::move(name)}
    , cql_statement_no_metadata(&timeout_config::truncate_timeout)
{
}

uint32_t truncate_statement::get_bound_terms() const
{
    return 0;
}

std::unique_ptr<prepared_statement> truncate_statement::prepare(database& db,cql_stats& stats)
{
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<truncate_statement>(*this));
}

bool truncate_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool truncate_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<> truncate_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    return state.has_column_family_access(proxy.local_db(), keyspace(), column_family(), auth::permission::MODIFY);
}

void truncate_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
#if 0
    ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
#endif
}

future<::shared_ptr<cql_transport::messages::result_message>>
truncate_statement::execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) const
{
    if (proxy.get_db().local().find_schema(keyspace(), column_family())->is_view()) {
        throw exceptions::invalid_request_exception("Cannot TRUNCATE materialized view directly; must truncate base table instead");
    }
    return proxy.truncate_blocking(keyspace(), column_family()).handle_exception([](auto ep) {
        throw exceptions::truncate_exception(ep);
    }).then([] {
        return ::shared_ptr<cql_transport::messages::result_message>{};
    });
}

audit::statement_category truncate_statement::category() const {
    return audit::statement_category::DML;
}

}

}
