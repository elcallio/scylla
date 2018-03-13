/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/thread.hh>
#include "utils/serialized_action.hh"
#include "tests/test-utils.hh"
#include "utils/phased_barrier.hh"
#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

SEASTAR_TEST_CASE(test_serialized_action_triggering) {
    return seastar::async([] {
        int current = 0;
        std::vector<int> history;
        promise<> p;

        serialized_action act([&] {
            auto val = current;
            return p.get_future().then([&, val] {
                history.push_back(val);
            });
        });

        auto release = [&] {
            std::exchange(p, promise<>()).set_value();
        };

        auto t1 = act.trigger();
        later().get(); // wait for t1 action to block
        current = 1;
        auto t2 = act.trigger();
        auto t3 = act.trigger();

        current = 2;
        release();

        t1.get();
        BOOST_REQUIRE(history.size() == 1);
        BOOST_REQUIRE(history.back() == 0);
        BOOST_REQUIRE(!t2.available());
        BOOST_REQUIRE(!t3.available());

        later().get(); // wait for t2 action to block
        current = 3;
        auto t4 = act.trigger();
        release();

        t2.get();
        t3.get();
        BOOST_REQUIRE(history.size() == 2);
        BOOST_REQUIRE(history.back() == 2);
        BOOST_REQUIRE(!t4.available());

        later().get(); // wait for t4 action to block
        current = 4;
        release();

        t4.get();
        BOOST_REQUIRE(history.size() == 3);
        BOOST_REQUIRE(history.back() == 3);

        current = 5;
        auto t5 = act.trigger();
        later().get(); // wait for t5 action to block
        release();
        t5.get();

        BOOST_REQUIRE(history.size() == 4);
        BOOST_REQUIRE(history.back() == 5);
    });
}
