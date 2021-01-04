/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "api.hh"

namespace netw { class messaging_service; }

namespace api {

void set_messaging_service(http_context& ctx, routes& r, sharded<netw::messaging_service>& ms);
void unset_messaging_service(http_context& ctx, routes& r);

}
