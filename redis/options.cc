/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "redis/options.hh"
#include "types.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "schema.hh"
#include "database.hh"
#include <seastar/core/print.hh>
#include "redis/keyspace_utils.hh"

using namespace seastar;

namespace redis {

schema_ptr get_schema(service::storage_proxy& proxy, const sstring& ks_name, const sstring& cf_name) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(ks_name, cf_name);
    return schema;
}

}
