/*
 * Copyright 2016 ScylaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once
#include "database_fwd.hh"
#include "service/storage_proxy.hh"
#include <seastar/http/httpd.hh>

namespace service { class load_meter; }
namespace locator { class shared_token_metadata; }
namespace cql_transport { class controller; }
class thrift_controller;
namespace db { class snapshot_ctl; }
namespace netw { class messaging_service; }

namespace api {

struct http_context {
    sstring api_dir;
    sstring api_doc;
    httpd::http_server_control http_server;
    distributed<database>& db;
    distributed<service::storage_proxy>& sp;
    service::load_meter& lmeter;
    const sharded<locator::shared_token_metadata>& shared_token_metadata;

    http_context(distributed<database>& _db,
            distributed<service::storage_proxy>& _sp,
            service::load_meter& _lm, const sharded<locator::shared_token_metadata>& _stm)
            : db(_db), sp(_sp), lmeter(_lm), shared_token_metadata(_stm) {
    }

    const locator::token_metadata& get_token_metadata();
};

future<> set_server_init(http_context& ctx);
future<> set_server_config(http_context& ctx);
future<> set_server_snitch(http_context& ctx);
future<> set_server_storage_service(http_context& ctx);
future<> set_server_repair(http_context& ctx, sharded<netw::messaging_service>& ms);
future<> unset_server_repair(http_context& ctx);
future<> set_transport_controller(http_context& ctx, cql_transport::controller& ctl);
future<> unset_transport_controller(http_context& ctx);
future<> set_rpc_controller(http_context& ctx, thrift_controller& ctl);
future<> unset_rpc_controller(http_context& ctx);
future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl);
future<> unset_server_snapshot(http_context& ctx);
future<> set_server_gossip(http_context& ctx);
future<> set_server_load_sstable(http_context& ctx);
future<> set_server_messaging_service(http_context& ctx, sharded<netw::messaging_service>& ms);
future<> unset_server_messaging_service(http_context& ctx);
future<> set_server_storage_proxy(http_context& ctx);
future<> set_server_stream_manager(http_context& ctx);
future<> set_server_gossip_settle(http_context& ctx);
future<> set_server_cache(http_context& ctx);
future<> set_server_done(http_context& ctx);

}
