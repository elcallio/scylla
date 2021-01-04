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
#include "storage_helper.hh"
#include "db/config.hh"

namespace audit {

class audit_syslog_storage_helper : public storage_helper {
    int _syslog_fd;
public:
    explicit audit_syslog_storage_helper(cql3::query_processor&) {};
    virtual ~audit_syslog_storage_helper();
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
