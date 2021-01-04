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
#include "seastar/core/distributed.hh"

using namespace seastar;

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
    distributed<redis::query_processor> _query_processor;
    shared_ptr<distributed<redis_transport::redis_server>> _server;
private:
    future<> listen(distributed<auth::service>& auth_service, db::config& cfg);
public:
    redis_service();
    ~redis_service();
    future<> init(distributed<service::storage_proxy>& proxy, distributed<database>& db, distributed<auth::service>& auth_service, db::config& cfg);
    future<> stop();
};
