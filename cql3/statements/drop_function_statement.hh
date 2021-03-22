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
#include "audit/audit.hh"

namespace cql3 {
class query_processor;
namespace statements {
class drop_function_statement final : public drop_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(
            query_processor& qp) const override;
protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override;

public:
    drop_function_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            bool args_present, bool if_exists);
};
}
}
