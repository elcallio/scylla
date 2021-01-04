/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "utils/fragment_range.hh"

namespace imr {

/// No-op deserialisation context
///
/// This is a dummy deserialisation context to be used when there is no need
/// for one, but the interface expects a context object.
static const struct no_context_t {
    template<typename Tag, typename... Args>
    const no_context_t& context_for(Args&&...) const noexcept { return *this; }
} no_context;

struct no_op_continuation {
    template<typename T>
    static T run(T value) noexcept {
        return value;
    }
};

template<typename T>
class placeholder {
    uint8_t* _pointer = nullptr;
public:
    placeholder() = default;
    explicit placeholder(uint8_t* ptr) noexcept : _pointer(ptr) { }
    void set_pointer(uint8_t* ptr) noexcept { _pointer = ptr; }

    template<typename... Args>
    void serialize(Args&&... args) noexcept {
        if (!_pointer) {
            // We lose the information whether we are in the sizing or
            // serializing phase, hence the need for this run-time check.
            return;
        }
        T::serialize(_pointer, std::forward<Args>(args)...);
    }
};

}
