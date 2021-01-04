/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "selector.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

lw_shared_ptr<column_specification>
selector::factory::get_column_specification(const schema& schema) const {
    return make_lw_shared<column_specification>(schema.ks_name(),
        schema.cf_name(),
        ::make_shared<column_identifier>(column_name(), true),
        get_return_type());
}

bool selector::requires_thread() const { return false; }
}

}


