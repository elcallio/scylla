/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "mutation_partition_visitor.hh"
#include "hashing.hh"
#include "schema.hh"
#include "atomic_cell_hash.hh"
#include "keys.hh"
#include "counters.hh"

// Calculates a hash of a mutation_partition which is consistent with
// mutation equality. For any equal mutations, no matter which schema
// version they were generated under, the hash fed will be the same for both of them.
template<typename Hasher>
class hashing_partition_visitor : public mutation_partition_visitor {
    Hasher& _h;
    const schema& _s;
public:
    hashing_partition_visitor(Hasher& h, const schema& s)
        : _h(h)
        , _s(s)
    { }

    virtual void accept_partition_tombstone(tombstone t) {
        feed_hash(_h, t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) {
        auto&& col = _s.static_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view cell) {
        auto&& col = _s.static_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }

    virtual void accept_row_tombstone(const range_tombstone& rt) {
        rt.feed_hash(_h, _s);
    }

    virtual void accept_row(position_in_partition_view pos, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
        if (dummy) {
            return;
        }
        pos.key().feed_hash(_h, _s);
        feed_hash(_h, deleted_at);
        feed_hash(_h, rm);
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) {
        auto&& col = _s.regular_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view cell) {
        auto&& col = _s.regular_column_at(id);
        feed_hash(_h, col.name());
        feed_hash(_h, col.type->name());
        feed_hash(_h, cell, col);
    }
};
