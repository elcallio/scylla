/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include "audit/audit.hh"
#include "table_helper.hh"
#include "storage_helper.hh"
#include "db/config.hh"

namespace cql3 {

class query_processor;

}

namespace audit {

class audit_cf_storage_helper : public storage_helper {
    static const sstring KEYSPACE_NAME;
    static const sstring TABLE_NAME;
    cql3::query_processor& _qp;
    table_helper _table;
    service::query_state _dummy_query_state;
    static cql3::query_options make_data(const audit_info* audit_info,
                                         socket_address node_ip,
                                         socket_address client_ip,
                                         db::consistency_level cl,
                                         const sstring& username,
                                         bool error);
    static cql3::query_options make_login_data(socket_address node_ip,
                                               socket_address client_ip,
                                               const sstring& username,
                                               bool error);
public:
    explicit audit_cf_storage_helper(cql3::query_processor& qp);
    virtual ~audit_cf_storage_helper() {}
    virtual future<> start(const db::config& cfg) override;
    virtual future<> stop() override;
    virtual future<> write(const audit_info* audit_info,
                           socket_address node_ip,
                           socket_address client_ip,
                           db::consistency_level cl,
                           const sstring& username,
                           bool error) override;
    virtual future<> write_login(const sstring& username,
                                 socket_address node_ip,
                                 socket_address client_ip,
                                 bool error) override;
};

}
