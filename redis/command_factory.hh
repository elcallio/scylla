/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "bytes.hh"
#include "seastar/core/shared_ptr.hh"
#include "abstract_command.hh"
#include "redis/options.hh"

namespace service {
    class storage_proxy;
}

namespace redis {
using namespace seastar;
class request;
class command_factory {
public:
    command_factory() {}
    ~command_factory() {}
    static future<redis_message> create_execute(service::storage_proxy&, request&, redis::redis_options&, service_permit);
};
}
