/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <algorithm>
#include "local_strategy.hh"
#include "utils/class_registrator.hh"
#include "utils/fb_utilities.hh"


namespace locator {

local_strategy::local_strategy(const sstring& keyspace_name, const shared_token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options) :
        abstract_replication_strategy(keyspace_name, token_metadata, snitch, config_options, replication_strategy_type::local) {}

std::vector<inet_address> local_strategy::do_get_natural_endpoints(const token& t, const token_metadata& tm, can_yield can_yield) {
    return calculate_natural_endpoints(t, tm, can_yield);
}

std::vector<inet_address> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm, can_yield) const {
    return std::vector<inet_address>({utils::fb_utilities::get_broadcast_address()});
}

void local_strategy::validate_options() const {
}

std::optional<std::set<sstring>> local_strategy::recognized_options() const {
    // LocalStrategy doesn't expect any options.
    return {};
}

size_t local_strategy::get_replication_factor() const {
    return 1;
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, const sstring&, const shared_token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
