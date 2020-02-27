/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/print.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/distributed.hh>
#include "seastarx.hh"

#include <chrono>
#include <iosfwd>
#include <boost/range/irange.hpp>

template <typename Func>
static
void time_it(Func func, int iterations = 5, int iterations_between_clock_readings = 1000) {
    using clk = std::chrono::steady_clock;

    for (int i = 0; i < iterations; i++) {
        auto start = clk::now();
        auto end_at = start + std::chrono::seconds(1);
        uint64_t count = 0;

        while (clk::now() < end_at) {
            for (int i = 0; i < iterations_between_clock_readings; i++) { // amortize clock reading cost
                func();
                count++;
            }
        }

        auto end = clk::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        std::cout << format("{:.2f}", (double)count / duration) << " tps\n";
    }
}

// Drives concurrent and continuous execution of given asynchronous action
// until a deadline. Counts invocations.
template <typename Func>
class executor {
    const Func _func;
    const lowres_clock::time_point _end_at;
    const uint64_t _end_at_count;
    const unsigned _n_workers;
    uint64_t _count;
private:
    future<> run_worker() {
        return do_until([this] {
            return _end_at_count ? _count == _end_at_count : lowres_clock::now() >= _end_at;
        }, [this] () mutable {
            ++_count;
            return _func();
        });
    }
public:
    executor(unsigned n_workers, Func func, lowres_clock::time_point end_at, uint64_t end_at_count = 0)
            : _func(std::move(func))
            , _end_at(end_at)
            , _end_at_count(end_at_count)
            , _n_workers(n_workers)
            , _count(0)
    { }

    // Returns the number of invocations of @func
    future<uint64_t> run() {
        auto idx = boost::irange(0, (int)_n_workers);
        return parallel_for_each(idx.begin(), idx.end(), [this] (auto idx) mutable {
            return this->run_worker();
        }).then([this] {
            return _count;
        });
    }

    future<> stop() {
        return make_ready_future<>();
    }
};

/**
 * Measures throughput of an asynchronous action. Executes the action on all cores
 * in parallel, with given number of concurrent executions per core.
 *
 * Runs many iterations. Prints partial total throughput after each iteraton.
 *
 * Returns a vector of throughputs achieved in each iteration.
 */
template <typename Func>
static
std::vector<double> time_parallel(Func func, unsigned concurrency_per_core, int iterations = 5, unsigned operations_per_shard = 0) {
    using clk = std::chrono::steady_clock;
    if (operations_per_shard) {
        iterations = 1;
    }
    std::vector<double> results;
    for (int i = 0; i < iterations; ++i) {
        auto start = clk::now();
        auto end_at = lowres_clock::now() + std::chrono::seconds(1);
        distributed<executor<Func>> exec;
        exec.start(concurrency_per_core, func, std::move(end_at), operations_per_shard).get();
        auto total = exec.map_reduce(adder<uint64_t>(), [] (auto& oc) { return oc.run(); }).get0();
        auto end = clk::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        auto result = static_cast<double>(total) / duration;
        std::cout << format("{:.2f}", result) << " tps\n";
        results.emplace_back(result);
        exec.stop().get();
    }
    return results;
}
