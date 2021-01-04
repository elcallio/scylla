/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "data/cell.hh"

#include "types.hh"

thread_local imr::alloc::context_factory<data::cell::last_chunk_context> lcc;
thread_local imr::alloc::lsa_migrate_fn<data::cell::external_last_chunk,
        imr::alloc::context_factory<data::cell::last_chunk_context>> data::cell::lsa_last_chunk_migrate_fn(lcc);
thread_local imr::alloc::context_factory<data::cell::chunk_context> ecc;
thread_local imr::alloc::lsa_migrate_fn<data::cell::external_chunk,
        imr::alloc::context_factory<data::cell::chunk_context>> data::cell::lsa_chunk_migrate_fn(ecc);

int compare_unsigned(data::value_view lhs, data::value_view rhs) noexcept
{
    auto it1 = lhs.begin();
    auto it2 = rhs.begin();
    while (it1 != lhs.end() && it2 != rhs.end()) {
        auto r = ::compare_unsigned(*it1, *it2);
        if (r) {
            return r;
        }
        ++it1;
        ++it2;
    }
    if (it1 != lhs.end()) {
        return 1;
    } else if (it2 != rhs.end()) {
        return -1;
    }
    return 0;
}

