/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "transport/messages/result_message.hh"

SEASTAR_TEST_CASE(test_index_with_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();

        sstring big_string(4096, 'j');
        // There should be enough rows to use multiple pages
        for (int i = 0; i < 64 * 1024; ++i) {
            e.execute_cql(format("INSERT INTO tab (pk, ck, v, v2, v3) VALUES ({}, 'hello{}', 1, {}, '{}')", i % 3, i, i, big_string)).get();
        }

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, infinite_timeout_config, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{4321, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            assert_that(res).is_rows().with_size(4321);
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1").get0();
            assert_that(res).is_rows().with_size(64 * 1024);
        });
    });
}
