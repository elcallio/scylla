/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <vector>
#include "gms/inet_address.hh"
#include "db/system_distributed_keyspace.hh"
#include "repair/repair.hh"
#include <seastar/core/distributed.hh>

class row_level_repair_gossip_helper;

namespace gms {
    class gossiper;
}

struct repair_service {
    distributed<gms::gossiper>& _gossiper;
    shared_ptr<row_level_repair_gossip_helper> _gossip_helper;
    tracker _tracker;
    repair_service(distributed<gms::gossiper>& gossiper, size_t max_repair_memory);
    ~repair_service();
    future<> stop();
private:
    bool _stopped = false;
};

future<> row_level_repair_init_messaging_service_handler(repair_service& rs, distributed<db::system_distributed_keyspace>& sys_dist_ks,
        distributed<db::view::view_update_generator>& view_update_generator, sharded<netw::messaging_service>& ms);
future<> row_level_repair_uninit_messaging_service_handler();

class repair_info;

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, utils::UUID table_id, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes);

future<> shutdown_all_row_level_repair();
