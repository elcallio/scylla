/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <chrono>

namespace runtime {

void init_uptime();

std::chrono::steady_clock::time_point get_boot_time();

/// Returns the uptime of the system.
std::chrono::steady_clock::duration get_uptime();

}
