/*
 * Copyright 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

//
// For reference, see https://developers.google.com/protocol-buffers/docs/encoding#varints
//

#pragma once

#include "bytes.hh"

#include <cstdint>

using vint_size_type = bytes::size_type;

static constexpr size_t max_vint_length = 9;

struct unsigned_vint final {
    using value_type = uint64_t;

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static value_type deserialize(bytes_view v);

    static vint_size_type serialized_size_from_first_byte(bytes::value_type first_byte);
};

struct signed_vint final {
    using value_type = int64_t;

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static value_type deserialize(bytes_view v);

    static vint_size_type serialized_size_from_first_byte(bytes::value_type first_byte);
};
