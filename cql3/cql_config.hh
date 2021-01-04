/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */



#pragma once

#include "restrictions/restrictions_config.hh"

namespace cql3 {

struct cql_config {
    restrictions::restrictions_config restrictions;
};

extern const cql_config default_cql_config;

}
