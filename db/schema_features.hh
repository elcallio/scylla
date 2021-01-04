/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "enum_set.hh"

namespace db {

enum class schema_feature {
    VIEW_VIRTUAL_COLUMNS,

    // When set, the schema digest is calcualted in a way such that it doesn't change after all
    // tombstones in an empty partition expire.
    // See https://github.com/scylladb/scylla/issues/4485
    DIGEST_INSENSITIVE_TO_EXPIRY,
    COMPUTED_COLUMNS,
    CDC_OPTIONS,
    PER_TABLE_PARTITIONERS,
    IN_MEMORY_TABLES,
};

using schema_features = enum_set<super_enum<schema_feature,
    schema_feature::VIEW_VIRTUAL_COLUMNS,
    schema_feature::DIGEST_INSENSITIVE_TO_EXPIRY,
    schema_feature::COMPUTED_COLUMNS,
    schema_feature::CDC_OPTIONS,
    schema_feature::PER_TABLE_PARTITIONERS,
    schema_feature::IN_MEMORY_TABLES
    >>;

}
