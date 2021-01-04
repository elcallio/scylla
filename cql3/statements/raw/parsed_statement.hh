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

#include "cql3/variable_specifications.hh"
#include "cql3/column_identifier.hh"
#include "cql3/stats.hh"

#include <seastar/core/shared_ptr.hh>

#include <optional>
#include <vector>
#include "audit/audit.hh"

namespace cql3 {

namespace statements {

class prepared_statement;

namespace raw {

class parsed_statement {
protected:
    variable_specifications _variables;

public:
    virtual ~parsed_statement();

    variable_specifications& get_bound_variables();
    const variable_specifications& get_bound_variables() const;

    void set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names);

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) = 0;

protected:
    virtual audit::statement_category category() const = 0;
    virtual audit::audit_info_ptr audit_info() const = 0;
};

}

}

}
