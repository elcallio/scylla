/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

// Not part of atomic_cell.hh to avoid cyclic dependency between types.hh and atomic_cell.hh

#include "types.hh"
#include "atomic_cell.hh"
#include "hashing.hh"
#include "counters.hh"

template<>
struct appending_hash<collection_mutation_view> {
    template<typename Hasher>
    void operator()(Hasher& h, collection_mutation_view cell, const column_definition& cdef) const {
        auto m_view = collection_type_impl::deserialize_mutation_form(cell);
        ::feed_hash(h, m_view.tomb);
        for (auto&& key_and_value : m_view.cells) {
            ::feed_hash(h, key_and_value.first);
            ::feed_hash(h, key_and_value.second, cdef);
        }
    }
};

template<>
struct appending_hash<atomic_cell_view> {
    template<typename Hasher>
    void operator()(Hasher& h, atomic_cell_view cell, const column_definition& cdef) const {
        feed_hash(h, cell.is_live());
        feed_hash(h, cell.timestamp());
        if (cell.is_live()) {
            if (cdef.is_counter()) {
                ::feed_hash(h, counter_cell_view(cell));
                return;
            }
            if (cell.is_live_and_has_ttl()) {
                feed_hash(h, cell.expiry());
                feed_hash(h, cell.ttl());
            }
            feed_hash(h, cell.value());
        } else {
            feed_hash(h, cell.deletion_time());
        }
    }
};

template<>
struct appending_hash<atomic_cell> {
    template<typename Hasher>
    void operator()(Hasher& h, const atomic_cell& cell, const column_definition& cdef) const {
        feed_hash(h, static_cast<atomic_cell_view>(cell), cdef);
    }
};

template<>
struct appending_hash<collection_mutation> {
    template<typename Hasher>
    void operator()(Hasher& h, const collection_mutation& cm, const column_definition& cdef) const {
        feed_hash(h, static_cast<collection_mutation_view>(cm), cdef);
    }
};
