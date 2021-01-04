/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/function_statement.hh"
#include "cql3/functions/user_function.hh"
#include "audit/audit.hh"

namespace cql3 {
namespace statements {
class create_function_statement final : public create_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(
            service::storage_proxy& proxy) const override;
    virtual void create(service::storage_proxy& proxy, functions::function* old) const override;
    sstring _language;
    sstring _body;
    std::vector<shared_ptr<column_identifier>> _arg_names;
    shared_ptr<cql3_type::raw> _return_type;
    bool _called_on_null_input;

    // To support "IF NOT EXISTS" we create the function during the verify stage and use it in announce_migration. In
    // this case it is possible that there is no error but no function is created. We could duplicate some logic in
    // announce_migration or have a _should_create boolean, but creating the function early is probably the simplest.
    mutable shared_ptr<functions::user_function> _func{};
protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override;
public:
    create_function_statement(functions::function_name name, sstring language, sstring body,
            std::vector<shared_ptr<column_identifier>> arg_names, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            shared_ptr<cql3_type::raw> return_type, bool called_on_null_input, bool or_replace, bool if_not_exists);
};
}
}
