/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>


using namespace seastar;

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
    distributed<database>& _db;
    seastar::metrics::metric_groups _metrics;
    seastar::gate _pending_command_gate;
public:
    query_processor(service::storage_proxy& proxy, distributed<database>& db);

    ~query_processor();

    distributed<database>& db() {
        return _db;
    }

    service::storage_proxy& proxy() {
        return _proxy;
    }

    future<redis_message> process(request&&, redis_options&, service_permit);

    future<> start();
    future<> stop();
};

}
