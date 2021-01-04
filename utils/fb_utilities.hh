/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cstdint>
#include <optional>
#include "gms/inet_address.hh"

namespace utils {

using inet_address = gms::inet_address;

class fb_utilities {
private:
    static std::optional<inet_address>& broadcast_address() noexcept {
        static std::optional<inet_address> _broadcast_address;

        return _broadcast_address;
    }
    static std::optional<inet_address>& broadcast_rpc_address() noexcept {
        static std::optional<inet_address> _broadcast_rpc_address;

        return _broadcast_rpc_address;
    }
public:
   static constexpr int32_t MAX_UNSIGNED_SHORT = 0xFFFF;

   static void set_broadcast_address(inet_address addr) noexcept {
       broadcast_address() = addr;
   }

   static void set_broadcast_rpc_address(inet_address addr) noexcept {
       broadcast_rpc_address() = addr;
   }


   static const inet_address get_broadcast_address() noexcept {
       assert(broadcast_address());
       return *broadcast_address();
   }

   static const inet_address get_broadcast_rpc_address() noexcept {
       assert(broadcast_rpc_address());
       return *broadcast_rpc_address();
   }

    static bool is_me(gms::inet_address addr) noexcept {
        return addr == get_broadcast_address();
    }
};
}
