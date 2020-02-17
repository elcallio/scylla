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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <iomanip>
#include <boost/io/ios_state.hpp>
#include <seastar/net/inet_address.hh>
#include <seastar/net/dns.hh>
#include <seastar/core/print.hh>
#include <seastar/core/future.hh>
#include "inet_address.hh"
#include "to_string.hh"

using namespace seastar;

future<gms::inet_address> gms::inet_address::lookup(sstring name, opt_family family, opt_family preferred) {
    return seastar::net::dns::get_host_by_name(name, family).then([preferred](seastar::net::hostent&& h) {
        for (auto& addr : h.addr_list) {
            if (!preferred || addr.in_family() == preferred) {
                return gms::inet_address(addr);
            }
        }
        return gms::inet_address(h.addr_list.front());
    });
}

std::ostream& gms::operator<<(std::ostream& os, const inet_address& x) {
    if (x.addr().is_ipv4()) {
        return os << x.addr();
    }

    boost::io::ios_flags_saver fs(os);

    os << std::hex;
    auto&& bytes = x.bytes();
    auto i = 0u;
    auto acc = 0u;
    // extra paranoid sign extension evasion - #5808
    for (uint8_t b : bytes) {
        acc <<= 8;
        acc |= b;
        if ((++i & 1) == 0) {
            os << (std::exchange(acc, 0u) & 0xffff);
            if (i != bytes.size()) {
                os << ":";
            }
        }
    }    
    os << std::dec;
    if (x.addr().scope() != seastar::net::inet_address::invalid_scope) {
        os << '%' << x.addr().scope();
    }
    return os;
}