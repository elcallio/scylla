/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "sstable_version.hh"
#include "types.hh"

namespace sstables {

class sstable_version_constants_k_l final : public sstable_version_constants {
    static const sstable_version_constants::component_map_t create_component_map();
public:
    sstable_version_constants_k_l() = delete;
    static const sstable_version_constants::component_map_t _component_map;
};

}
