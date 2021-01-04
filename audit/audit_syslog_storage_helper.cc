/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "audit/audit_syslog_storage_helper.hh"

#include <sys/socket.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <fmt/chrono.h>
#include "utils/class_registrator.hh"

namespace cql3 {

class query_processor;

}

namespace audit {

audit_syslog_storage_helper::~audit_syslog_storage_helper() {
    if (_syslog_fd != -1) {
        close(_syslog_fd);
        _syslog_fd = -1;
    }
}

/*
 * We don't use openlog and syslog directly because it's already used by logger.
 * Audit needs to use different ident so than logger but syslog.h uses a global ident
 * and it's not possible to use more than one in a program.
 *
 * To work around it we directly communicate with the socket.
 */
future<> audit_syslog_storage_helper::start(const db::config& cfg) {
    sockaddr syslog_address;
    syslog_address.sa_family = AF_UNIX;
    strncpy(syslog_address.sa_data, _PATH_LOG, sizeof(syslog_address.sa_data));
    _syslog_fd = ::socket(AF_UNIX, SOCK_DGRAM, 0);
    if (_syslog_fd == -1) {
        throw audit_exception(sprint("Error creating socket to syslog (error %d)", errno));
    } else {
        int newMaxBuff= cfg.audit_syslog_write_buffer_size();
        setsockopt(_syslog_fd, SOL_SOCKET, SO_SNDBUF, &newMaxBuff, sizeof(newMaxBuff));
        fcntl(_syslog_fd, F_SETFD, FD_CLOEXEC);
        if (connect(_syslog_fd, &syslog_address, sizeof(syslog_address)) == -1) {
            close(_syslog_fd);
            _syslog_fd = -1;
            throw audit_exception(sprint("Error connecting to syslog (error %d)", errno));
        }
    }
    return make_ready_future<>();
}

future<> audit_syslog_storage_helper::stop() {
    if (_syslog_fd != -1) {
        close(_syslog_fd);
        _syslog_fd = -1;
    }
    return make_ready_future<>();
}

future<> audit_syslog_storage_helper::write(const audit_info* audit_info,
                                            socket_address node_ip,
                                            socket_address client_ip,
                                            db::consistency_level cl,
                                            const sstring& username,
                                            bool error) {
    if (_syslog_fd == -1) {
        logger.error("Can't log audit message to syslog. Socket not connected.");
    } else {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        tm time;
        localtime_r(&now, &time);
        sstring msg = seastar::format("<{}>{:%h %e %T} scylla-audit: \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\"",
                                      LOG_NOTICE | LOG_USER,
                                      time,
                                      node_ip,
                                      audit_info->category_string(),
                                      cl,
                                      audit_info->table(),
                                      audit_info->keyspace(),
                                      audit_info->query(),
                                      client_ip,
                                      username,
                                      (error ? "true" : "false"));
        if (send(_syslog_fd, msg.c_str(), msg.size(), 0) < 0) {
            logger.error("Can't log audit message to syslog. Send failed.");
        }
    }
    return make_ready_future<>();
}

future<> audit_syslog_storage_helper::write_login(const sstring& username,
                                                  socket_address node_ip,
                                                  socket_address client_ip,
                                                  bool error) {
    if (_syslog_fd == -1) {
        logger.error("Can't log audit message to syslog. Socket not connected.");
    } else {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        tm time;
        localtime_r(&now, &time);
        sstring msg = seastar::format("<{}>{:%h %e %T} scylla-audit: \"{}\", \"AUTH\", \"\", \"\", \"\", \"\", \"{}\", \"{}\", \"{}\"",
                                      LOG_NOTICE | LOG_USER,
                                      time,
                                      node_ip,
                                      client_ip,
                                      username,
                                      (error ? "true" : "false"));
        if (send(_syslog_fd, msg.c_str(), msg.size(), 0) < 0) {
            logger.error("Can't log audit message to syslog. Send failed.");
        }
    }
    return make_ready_future<>();
}

using registry = class_registrator<storage_helper, audit_syslog_storage_helper, cql3::query_processor&>;
static registry registrator1("audit_syslog_storage_helper");

}
