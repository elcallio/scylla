/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cstddef>

namespace data {

/// Type information
///
/// `type_info` keeps the type information relevant for the serialisation code.
/// In particular we need to distinguish between fixed-size and variable-sized
/// types. Collections and counters are considered to be variable-sized types.
///
/// \note Even if the type is fixed-size (e.g. `int32_type`) the value can be
/// empty and its length will be 0. This is a special (and rare) case handled
/// by the cell implementation and ignored by `type_info`.
class type_info {
    size_t _fixed_size;
private:
    explicit type_info(size_t size) noexcept : _fixed_size(size) { }
public:
    static type_info make_fixed_size(size_t size) noexcept {
        return type_info { size_t(size) };
    }
    static type_info make_variable_size() noexcept {
        return type_info { 0 };
    }
    static type_info make_collection() noexcept {
        return type_info { 0 };
    }

    /// Check whether the type is fixed-size.
    bool is_fixed_size() const noexcept {
        return _fixed_size > 0;
    }

    /// Get the size of the value of a fixed-size type.
    ///
    /// Valid only if `is_fixed_size()` returns `true`.
    size_t value_size() const noexcept {
        return _fixed_size;
    }
};

}
