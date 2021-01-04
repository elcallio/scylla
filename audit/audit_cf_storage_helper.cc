/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "audit/audit_cf_storage_helper.hh"

namespace audit {

const sstring audit_cf_storage_helper::KEYSPACE_NAME("audit");
const sstring audit_cf_storage_helper::TABLE_NAME("audit_log");

audit_cf_storage_helper::audit_cf_storage_helper(cql3::query_processor& qp)
    : _qp(qp)
    , _table(KEYSPACE_NAME, TABLE_NAME,
             sprint("CREATE TABLE IF NOT EXISTS %s.%s ("
                       "date timestamp, "
                       "node inet, "
                       "event_time timeuuid, "
                       "category text, "
                       "consistency text, "
                       "table_name text, "
                       "keyspace_name text, "
                       "operation text, "
                       "source inet, "
                       "username text, "
                       "error boolean, "
                       "PRIMARY KEY ((date, node), event_time))",
                       KEYSPACE_NAME, TABLE_NAME),
             sprint("INSERT INTO %s.%s ("
                       "date,"
                       "node,"
                       "event_time,"
                       "category,"
                       "consistency,"
                       "table_name,"
                       "keyspace_name,"
                       "operation,"
                       "source,"
                       "username,"
                       "error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       KEYSPACE_NAME, TABLE_NAME))
    , _dummy_query_state(service::client_state::for_internal_calls(), empty_service_permit())
{
}

future<> audit_cf_storage_helper::start(const db::config&) {
    return table_helper::setup_keyspace(_qp, KEYSPACE_NAME, "1", _dummy_query_state, { &_table });
}

future<> audit_cf_storage_helper::stop() {
    return make_ready_future<>();
}

future<> audit_cf_storage_helper::write(const audit_info* audit_info,
                                    socket_address node_ip,
                                    socket_address client_ip,
                                    db::consistency_level cl,
                                    const sstring& username,
                                    bool error) {
    return _table.insert(_qp, _dummy_query_state, make_data, audit_info, node_ip, client_ip, cl, username, error);
}

future<> audit_cf_storage_helper::write_login(const sstring& username,
                                              socket_address node_ip,
                                              socket_address client_ip,
                                              bool error) {
    return _table.insert(_qp, _dummy_query_state, make_login_data, node_ip, client_ip, username, error);
}

cql3::query_options audit_cf_storage_helper::make_data(const audit_info* audit_info,
                                                       socket_address node_ip,
                                                       socket_address client_ip,
                                                       db::consistency_level cl,
                                                       const sstring& username,
                                                       bool error) {
    auto time = std::chrono::system_clock::now();
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count();
    auto ticks_per_day = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::hours(24)).count();
    auto date = millis_since_epoch / ticks_per_day * ticks_per_day;
    thread_local static int64_t last_nanos = 0;
    auto time_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(last_nanos, time));
    std::stringstream consistency_level;
    consistency_level << cl;
    std::vector<cql3::raw_value> values {
        cql3::raw_value::make_value(timestamp_type->decompose(date)),
        cql3::raw_value::make_value(inet_addr_type->decompose(node_ip.addr())),
        cql3::raw_value::make_value(uuid_type->decompose(time_id)),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->category_string())),
        cql3::raw_value::make_value(utf8_type->decompose(sstring(consistency_level.str()))),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->table())),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->keyspace())),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->query())),
        cql3::raw_value::make_value(inet_addr_type->decompose(client_ip.addr())),
        cql3::raw_value::make_value(utf8_type->decompose(username)),
        cql3::raw_value::make_value(boolean_type->decompose(error)),
    };
    return cql3::query_options(cql3::default_cql_config, db::consistency_level::ONE, infinite_timeout_config, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest());
}

cql3::query_options audit_cf_storage_helper::make_login_data(socket_address node_ip,
                                                             socket_address client_ip,
                                                             const sstring& username,
                                                             bool error) {
    auto time = std::chrono::system_clock::now();
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count();
    auto ticks_per_day = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::hours(24)).count();
    auto date = millis_since_epoch / ticks_per_day * ticks_per_day;
    thread_local static int64_t last_nanos = 0;
    auto time_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(last_nanos, time));
    std::vector<cql3::raw_value> values {
            cql3::raw_value::make_value(timestamp_type->decompose(date)),
            cql3::raw_value::make_value(inet_addr_type->decompose(node_ip.addr())),
            cql3::raw_value::make_value(uuid_type->decompose(time_id)),
            cql3::raw_value::make_value(utf8_type->decompose(sstring("AUTH"))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring(""))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring(""))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring(""))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring("LOGIN"))),
            cql3::raw_value::make_value(inet_addr_type->decompose(client_ip.addr())),
            cql3::raw_value::make_value(utf8_type->decompose(username)),
            cql3::raw_value::make_value(boolean_type->decompose(error)),
    };
    return cql3::query_options(cql3::default_cql_config, db::consistency_level::ONE, infinite_timeout_config, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT, cql_serialization_format::latest());
}

using registry = class_registrator<storage_helper, audit_cf_storage_helper, cql3::query_processor&>;
static registry registrator1("audit_cf_storage_helper");

}
