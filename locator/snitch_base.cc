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
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "locator/snitch_base.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"

namespace locator {
std::optional<sstring>
snitch_base::get_endpoint_info(inet_address endpoint,
                               gms::application_state key) {
    gms::gossiper& local_gossiper = gms::get_local_gossiper();
    auto* ep_state = local_gossiper.get_application_state_ptr(endpoint, key);
    return ep_state ? std::optional(ep_state->value) : std::nullopt;
}

int snitch_base::get_shard_count(inet_address endpoint) {
    auto val = get_endpoint_info(endpoint,
                                 gms::application_state::SHARD_COUNT);
    return val ? std::stoi(*val) : -1;
}

unsigned snitch_base::get_ignore_msb_bits(inet_address endpoint) {
    auto val = get_endpoint_info(endpoint,
                                 gms::application_state::IGNORE_MSB_BITS);
    return val ? std::stoi(*val) : 0;
}

std::vector<inet_address> snitch_base::get_sorted_list_by_proximity(
    inet_address address,
    std::vector<inet_address>& unsorted_address) {

    std::vector<inet_address>
        preferred(unsorted_address.begin(), unsorted_address.end());

    sort_by_proximity(address, preferred);
    return preferred;
}

void snitch_base::sort_by_proximity(
    inet_address address, std::vector<inet_address>& addresses) {

    std::sort(addresses.begin(), addresses.end(),
              [this, &address](inet_address& a1, inet_address& a2)
    {
        return compare_endpoints(address, a1, a2) < 0;
    });
}

int snitch_base::compare_endpoints(
    inet_address& address, inet_address& a1, inet_address& a2) {

    //
    // if one of the Nodes IS the Node we are comparing to and the other one
    // IS NOT - then return the appropriate result.
    //
    if (address == a1 && address != a2) {
        return -1;
    }

    if (address == a2 && address != a1) {
        return 1;
    }

    // ...otherwise perform the similar check in regard to Data Center
    sstring address_datacenter = get_datacenter(address);
    sstring a1_datacenter = get_datacenter(a1);
    sstring a2_datacenter = get_datacenter(a2);

    if (address_datacenter == a1_datacenter &&
        address_datacenter != a2_datacenter) {
        return -1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter != a1_datacenter) {
        return 1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter == a1_datacenter) {
        //
        // ...otherwise (in case Nodes belong to the same Data Center) check
        // the racks they belong to.
        //
        sstring address_rack = get_rack(address);
        sstring a1_rack = get_rack(a1);
        sstring a2_rack = get_rack(a2);

        if (address_rack == a1_rack && address_rack != a2_rack) {
            return -1;
        }

        if (address_rack == a2_rack && address_rack != a1_rack) {
            return 1;
        }
    }
    //
    // We don't differentiate between Nodes if all Nodes belong to different
    // Data Centers, thus make them equal.
    //
    return 0;
}

bool snitch_base::is_worth_merging_for_range_query(
    std::vector<inet_address>& merged,
    std::vector<inet_address>& l1,
    std::vector<inet_address>& l2) {
    //
    // Querying remote DC is likely to be an order of magnitude slower than
    // querying locally, so 2 queries to local nodes is likely to still be
    // faster than 1 query involving remote ones
    //
    bool merged_has_remote = has_remote_node(merged);
    return merged_has_remote
        ? (has_remote_node(l1) || has_remote_node(l2))
        : true;
}

bool snitch_base::has_remote_node(std::vector<inet_address>& l) {
    for (auto&& ep : l) {
        if (_my_dc != get_datacenter(ep)) {
            return true;
        }
    }

    return false;
}

future<> i_endpoint_snitch::stop_snitch() {
    // First stop the instance on a CPU where I/O is running
    return snitch_instance().invoke_on(io_cpu_id(), [] (snitch_ptr& s) {
        return s->stop();
    }).then([] { return snitch_instance().stop(); });
}

} // namespace locator
