/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/sharded.hh"

namespace db {
class config;
};

namespace redis {
class query_processor;
}

namespace redis_transport {
class redis_server;
}

namespace auth {
class service;
}

namespace service {
class storage_proxy;
}

class database;

class redis_service {
    seastar::sharded<redis::query_processor> _query_processor;
    seastar::shared_ptr<seastar::sharded<redis_transport::redis_server>> _server;
private:
    seastar::future<> listen(seastar::sharded<auth::service>& auth_service, db::config& cfg);
public:
    redis_service();
    ~redis_service();
    seastar::future<> init(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<database>& db, seastar::sharded<auth::service>& auth_service, db::config& cfg);
    seastar::future<> stop();
};
