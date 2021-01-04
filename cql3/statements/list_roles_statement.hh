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
 * Copyright 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <optional>

#include <seastar/core/sstring.hh>

#include "cql3/statements/authorization_statement.hh"
#include "cql3/role_name.hh"
#include "seastarx.hh"

namespace cql3 {

namespace statements {

class list_roles_statement final : public authorization_statement {
    std::optional<sstring> _grantee;

    bool _recursive;

public:
    list_roles_statement(const std::optional<role_name>& grantee, bool recursive)
        : _grantee(grantee ? sstring(grantee->to_string()) : std::optional<sstring>()), _recursive(recursive) {}

    std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state&) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(service::storage_proxy&, service::query_state&, const query_options&) const override;
};

}

}
