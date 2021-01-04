/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once
#include "types.hh"

namespace redis {

    future<bytes> lolwut5(const int cols, const int squares_per_row, const int squares_per_col);

}
