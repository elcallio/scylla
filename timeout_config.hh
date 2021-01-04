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

#include "db/timeout_clock.hh"

struct timeout_config {
    db::timeout_clock::duration read_timeout;
    db::timeout_clock::duration write_timeout;
    db::timeout_clock::duration range_read_timeout;
    db::timeout_clock::duration counter_write_timeout;
    db::timeout_clock::duration truncate_timeout;
    db::timeout_clock::duration cas_timeout;
    db::timeout_clock::duration other_timeout;
};

using timeout_config_selector = db::timeout_clock::duration (timeout_config::*);

extern const timeout_config infinite_timeout_config;

namespace db { class config; }
timeout_config make_timeout_config(const db::config& cfg);
