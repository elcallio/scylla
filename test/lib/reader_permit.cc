/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "test/lib/reader_permit.hh"

namespace tests {

thread_local reader_concurrency_semaphore the_semaphore{reader_concurrency_semaphore::no_limits{}};

reader_concurrency_semaphore& semaphore() {
    return the_semaphore;
}

reader_permit make_permit() {
    return the_semaphore.make_permit(nullptr, "test");
}

query::query_class_config make_query_class_config() {
    return query::query_class_config{the_semaphore, query::max_result_size(std::numeric_limits<uint64_t>::max())};
}

} // namespace tests
