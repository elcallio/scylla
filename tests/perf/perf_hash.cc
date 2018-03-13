/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "utils/murmur_hash.hh"
#include "tests/perf/perf.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

volatile uint64_t black_hole;

int main(int argc, char* argv[]) {
    const uint64_t seed = 0;
    auto src = bytes("0123412308129301923019283056789012345");

    uint64_t sink = 0;

    std::cout << "Timing fixed hash...\n";

    time_it([&] {
        std::array<uint64_t,2> dst;
        utils::murmur_hash::hash3_x64_128(src, seed, dst);
        sink += dst[0];
        sink += dst[1];
    });

    std::cout << "Timing iterator hash...\n";

    time_it([&] {
        std::array<uint64_t,2> dst;
        utils::murmur_hash::hash3_x64_128(src.begin(), src.size(), seed, dst);
        sink += dst[0];
        sink += dst[1];
    });

    black_hole = sink;
}
