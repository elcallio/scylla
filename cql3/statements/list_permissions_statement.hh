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

#pragma once

#include <optional>

#include "authorization_statement.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"

namespace cql3 {

class query_processor;

namespace statements {

class list_permissions_statement : public authorization_statement {
private:
    auth::permission_set _permissions;
    mutable std::optional<auth::resource> _resource;
    std::optional<sstring> _role_name;
    bool _recursive;

public:
    list_permissions_statement(auth::permission_set, std::optional<auth::resource>, std::optional<sstring>, bool);

    std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    void validate(service::storage_proxy&, const service::client_state&) const override;

    future<> check_access(service::storage_proxy& proxy, const service::client_state&) const override;

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state& , const query_options&) const override;
};

}

}
