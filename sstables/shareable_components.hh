/*
 * Copyright (C) 2019 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "compress.hh"
#include "sstables/types.hh"
#include "utils/i_filter.hh"

namespace sstables {

// Immutable components that can be shared among shards.
struct shareable_components {
    sstables::compression compression;
    utils::filter_ptr filter;
    sstables::summary summary;
    sstables::statistics statistics;
    std::optional<sstables::scylla_metadata> scylla_metadata;
};

}   // namespace sstables
