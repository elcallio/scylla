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

#include "authentication_statement.hh"
#include "cql3/user_options.hh"

namespace cql3 {

namespace statements {

class create_user_statement : public authentication_statement {
private:
    sstring _username;
    ::shared_ptr<user_options> _opts;
    bool _superuser;
    bool _if_not_exists;
public:

    create_user_statement(sstring, ::shared_ptr<user_options>, bool superuser, bool if_not_exists);

    void validate(distributed<service::storage_proxy>&, const service::client_state&) override;

    future<::shared_ptr<cql_transport::messages::result_message>> execute(distributed<service::storage_proxy>&
                    , service::query_state&
                    , const query_options&) override;
};

}

}
