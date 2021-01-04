/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */



#pragma once

#include <cstdint>

namespace cql3::restrictions {

struct restrictions_config {
    uint32_t partition_key_restrictions_max_cartesian_product_size = 100;
    uint32_t clustering_key_restrictions_max_cartesian_product_size = 100;
};

}
