/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <optional>

#include "timestamp.hh"

struct mutation_source_metadata {
    std::optional<api::timestamp_type> min_timestamp;
    std::optional<api::timestamp_type> max_timestamp;
};
