/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "gms/inet_address.hh"
#include <cstdint>

namespace netw {

struct msg_addr {
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const msg_addr& x, const msg_addr& y) noexcept;
    friend bool operator<(const msg_addr& x, const msg_addr& y) noexcept;
    friend std::ostream& operator<<(std::ostream& os, const msg_addr& x);
    struct hash {
        size_t operator()(const msg_addr& id) const noexcept;
    };
    explicit msg_addr(gms::inet_address ip) noexcept : addr(ip), cpu_id(0) { }
    msg_addr(gms::inet_address ip, uint32_t cpu) noexcept : addr(ip), cpu_id(cpu) { }
};

}
