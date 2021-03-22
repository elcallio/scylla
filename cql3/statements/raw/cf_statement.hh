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

#pragma once

#include "cql3/cf_name.hh"

#include <optional>

#include "parsed_statement.hh"

namespace service { class client_state; }

namespace cql3 {

namespace statements {

namespace raw {

/**
 * Abstract class for statements that apply on a given column family.
 */
class cf_statement : public parsed_statement {
protected:
    std::optional<cf_name> _cf_name;

    cf_statement(std::optional<cf_name> cf_name);
public:
    virtual void prepare_keyspace(const service::client_state& state);

    // Only for internal calls, use the version with ClientState for user queries
    void prepare_keyspace(std::string_view keyspace);

    virtual const sstring& keyspace() const;

    virtual const sstring& column_family() const;

    virtual audit::audit_info_ptr audit_info() const override {
        return audit::audit::create_audit_info(category(), keyspace(), column_family());
    }
};

}

}

}
