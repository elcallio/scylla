/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

namespace sstables {

enum class compaction_strategy_type {
    null,
    major,
    size_tiered,
    leveled,
    date_tiered,
    time_window,
    in_memory,
    incremental,
};

enum class reshape_mode { strict, relaxed };
}
