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
#include "locator/abstract_replication_strategy.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

schema_altering_statement::schema_altering_statement(timeout_config_selector timeout_selector)
    : cf_statement(cf_name())
    , cql_statement_no_metadata(timeout_selector)
    , _is_column_family_level{false}
{
}

schema_altering_statement::schema_altering_statement(cf_name name, timeout_config_selector timeout_selector)
    : cf_statement{std::move(name)}
    , cql_statement_no_metadata(timeout_selector)
    , _is_column_family_level{true}
{
}

future<> schema_altering_statement::grant_permissions_to_creator(const service::client_state&) const {
    return make_ready_future<>();
}

bool schema_altering_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool schema_altering_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

uint32_t schema_altering_statement::get_bound_terms() const
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
schema_altering_statement::execute0(query_processor& qp, service::query_state& state, const query_options& options) const {
    // If an IF [NOT] EXISTS clause was used, this may not result in an actual schema change.  To avoid doing
    // extra work in the drivers to handle schema changes, we return an empty message in this case. (CASSANDRA-7600)
    return announce_migration(qp).then([this] (auto ce) {
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
schema_altering_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    bool internal = state.get_client_state().is_internal();
    if (internal) {
        auto replication_type = locator::replication_strategy_type::everywhere_topology;
        database& db = qp.db();
        if (_cf_name && _cf_name->has_keyspace()) {
           const auto& ks = db.find_keyspace(_cf_name->get_keyspace());
           replication_type = ks.get_replication_strategy().get_type();
        }
        if (replication_type != locator::replication_strategy_type::local) {
            sstring info = _cf_name ? _cf_name->to_string() : "schema";
            throw std::logic_error(format("Attempted to modify {} via internal query: such schema changes are not propagated and thus illegal", info));
        }
    }

    return execute0(qp, state, options).then([this, &state, internal](::shared_ptr<messages::result_message> result) {
        auto permissions_granted_fut = internal
                ? make_ready_future<>()
                : grant_permissions_to_creator(state.get_client_state());
        return permissions_granted_fut.then([result = std::move(result)] {
           return result;
        });
    });
}

audit::statement_category schema_altering_statement::category() const {
    return audit::statement_category::DDL;
}

}

}
