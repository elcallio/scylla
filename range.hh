/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "interval.hh"

// range.hh is deprecated and should be replaced with interval.hh


template <typename T>
using range_bound = interval_bound<T>;

template <typename T>
using nonwrapping_range = interval<T>;

template <typename T>
using wrapping_range = wrapping_interval<T>;

template <typename T>
using range = wrapping_interval<T>;

template <template<typename> typename T, typename U>
concept Range = Interval<T, U>;
