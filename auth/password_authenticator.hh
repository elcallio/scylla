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
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/abort_source.hh>

#include "auth/authenticator.hh"
#include "cql3/query_processor.hh"

namespace service {
class migration_manager;
}

namespace auth {

const sstring& password_authenticator_name();

class password_authenticator : public authenticator {
    cql3::query_processor& _qp;
    ::service::migration_manager& _migration_manager;
    future<> _stopped;
    seastar::abort_source _as;

public:
    static db::consistency_level consistency_for_user(std::string_view role_name);

    password_authenticator(cql3::query_processor&, ::service::migration_manager&);

    ~password_authenticator();

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual std::string_view qualified_java_name() const override;

    virtual bool require_authentication() const override;

    virtual authentication_option_set supported_options() const override;

    virtual authentication_option_set alterable_options() const override;

    virtual future<authenticated_user> authenticate(const credentials_map& credentials) const override;

    virtual future<> create(std::string_view role_name, const authentication_options& options) const override;

    virtual future<> alter(std::string_view role_name, const authentication_options& options) const override;

    virtual future<> drop(std::string_view role_name) const override;

    virtual future<custom_options> query_custom_options(std::string_view role_name) const override;

    virtual const resource_set& protected_resources() const override;

    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;

private:
    bool legacy_metadata_exists() const;

    future<> migrate_legacy_metadata() const;

    future<> create_default_if_missing() const;
};

}

