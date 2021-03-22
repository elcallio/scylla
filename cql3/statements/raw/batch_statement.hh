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
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/cql_statement.hh"
#include "modification_statement.hh"
#include "service/storage_proxy.hh"
#include "transport/messages/result_message.hh"
#include "timestamp.hh"
#include "log.hh"
#include "to_string.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/uniqued.hpp>
#include <boost/iterator/counting_iterator.hpp>

#pragma once

namespace cql3 {

namespace statements {

namespace raw {

class batch_statement : public raw::cf_statement {
public:
    enum class type {
        LOGGED, UNLOGGED, COUNTER
    };
private:
    type _type;
    std::unique_ptr<attributes::raw> _attrs;
    std::vector<std::unique_ptr<raw::modification_statement>> _parsed_statements;
public:
    batch_statement(
        type type_,
        std::unique_ptr<attributes::raw> attrs,
        std::vector<std::unique_ptr<raw::modification_statement>> parsed_statements)
            : cf_statement(std::nullopt)
            , _type(type_)
            , _attrs(std::move(attrs))
            , _parsed_statements(std::move(parsed_statements)) {
    }

    virtual void prepare_keyspace(const service::client_state& state) override {
        for (auto&& s : _parsed_statements) {
            s->prepare_keyspace(state);
        }
    }

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override {
        // We don't audit batch statements. Instead we audit statements that are inside the batch.
        return audit::audit::create_no_audit_info();
    }
};

}

}
}
