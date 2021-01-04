/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string_view>
#include <functional>
#include <iosfwd>
#include <optional>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace auth {

class role_or_anonymous final {
public:
    std::optional<sstring> name{};

    role_or_anonymous() = default;
    role_or_anonymous(std::string_view name) : name(name) {
    }
};

std::ostream& operator<<(std::ostream&, const role_or_anonymous&);

bool operator==(const role_or_anonymous&, const role_or_anonymous&) noexcept;

inline bool operator!=(const role_or_anonymous& mr1, const role_or_anonymous& mr2) noexcept {
    return !(mr1 == mr2);
}

bool is_anonymous(const role_or_anonymous&) noexcept;

}

namespace std {

template <>
struct hash<auth::role_or_anonymous> {
    size_t operator()(const auth::role_or_anonymous& mr) const {
        return hash<std::optional<sstring>>()(mr.name);
    }
};

}
