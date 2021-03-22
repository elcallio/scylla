/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>

class database;
class service_permit;

namespace service {
class storage_proxy;
}

namespace redis {

class redis_options;
struct request;
struct reply;
class redis_message;

class query_processor {
    service::storage_proxy& _proxy;
    seastar::sharded<database>& _db;
    seastar::metrics::metric_groups _metrics;
    seastar::gate _pending_command_gate;
public:
    query_processor(service::storage_proxy& proxy, seastar::sharded<database>& db);

    ~query_processor();

    seastar::sharded<database>& db() {
        return _db;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    seastar::future<redis_message> process(request&&, redis_options&, service_permit);

    seastar::future<> start();
    seastar::future<> stop();
};

}
