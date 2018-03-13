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

#include "cql3/statements/drop_table_statement.hh"
#include "cql3/statements/prepared_statement.hh"

#include "service/migration_manager.hh"

namespace cql3 {

namespace statements {

drop_table_statement::drop_table_statement(::shared_ptr<cf_name> cf_name, bool if_exists)
    : schema_altering_statement{std::move(cf_name)}
    , _if_exists{if_exists}
{
}

future<> drop_table_statement::check_access(const service::client_state& state)
{
    // invalid_request_exception is only thrown synchronously.
    try {
        return state.has_column_family_access(keyspace(), column_family(), auth::permission::DROP);
    } catch (exceptions::invalid_request_exception&) {
        if (!_if_exists) {
            throw;
        }
        return make_ready_future();
    }
}

void drop_table_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state)
{
    // validated in announce_migration()
}

future<shared_ptr<cql_transport::event::schema_change>> drop_table_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only)
{
    return make_ready_future<>().then([this, is_local_only] {
        return service::get_local_migration_manager().announce_column_family_drop(keyspace(), column_family(), is_local_only);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;
            return make_shared<event::schema_change>(
                    event::schema_change::change_type::DROPPED,
                    event::schema_change::target_type::TABLE,
                    this->keyspace(),
                    this->column_family());
        } catch (const exceptions::configuration_exception& e) {
            if (_if_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_table_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<drop_table_statement>(*this));
}

}

}
