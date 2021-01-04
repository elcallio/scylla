/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <ranges>

namespace ranges {

#ifndef __clang__
template <std::ranges::range Container, std::ranges::range Range>
#else
// 'Range' should be constrained to std::ranges::range, but due to
// problems between clang and <ranges>, it cannot be
template <std::ranges::range Container, typename Range>
#endif
Container to(const Range& range) {
    return Container(range.begin(), range.end());
}

}
