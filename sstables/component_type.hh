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

#include <iosfwd>

namespace sstables {

enum class component_type {
    Index,
    CompressionInfo,
    Data,
    TOC,
    Summary,
    Digest,
    CRC,
    Filter,
    Statistics,
    TemporaryTOC,
    TemporaryStatistics,
    Scylla,
    Unknown,
};

std::ostream& operator<<(std::ostream&, const sstables::component_type&);

}

using component_type = ::sstables::component_type;
