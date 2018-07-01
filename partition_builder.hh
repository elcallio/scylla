/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "mutation_partition.hh"
#include "mutation_partition_view.hh"

// Partition visitor which builds mutation_partition corresponding to the data its fed with.
class partition_builder final : public mutation_partition_visitor {
private:
    const schema& _schema;
    mutation_partition& _partition;
    deletable_row* _current_row;
public:
    // @p will hold the result of building.
    // @p must be empty.
    partition_builder(const schema& s, mutation_partition& p)
        : _schema(s)
        , _partition(p)
    { }

    virtual void accept_partition_tombstone(tombstone t) override {
        _partition.apply(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema.static_column_at(id);
        accept_static_cell(id, atomic_cell(*cdef.type, cell));
    }

    void accept_static_cell(column_id id, atomic_cell&& cell) {
        row& r = _partition.static_row();
        r.append_cell(id, atomic_cell_or_collection(std::move(cell)));
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override {
        row& r = _partition.static_row();
        auto& ctype = *static_pointer_cast<const collection_type_impl>(_schema.static_column_at(id).type);
        r.append_cell(id, atomic_cell_or_collection(collection_mutation(ctype, collection)));
    }

    virtual void accept_row_tombstone(const range_tombstone& rt) override {
        _partition.apply_row_tombstone(_schema, rt);
    }

    virtual void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
        deletable_row& r = _partition.clustered_row(_schema, key, dummy, continuous);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema.regular_column_at(id);
        accept_row_cell(id, atomic_cell(*cdef.type, cell));
    }

    void accept_row_cell(column_id id, atomic_cell&& cell) {
        row& r = _current_row->cells();
        r.append_cell(id, atomic_cell_or_collection(std::move(cell)));
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override {
        row& r = _current_row->cells();
            auto& ctype = *static_pointer_cast<const collection_type_impl>(_schema.regular_column_at(id).type);
        r.append_cell(id, atomic_cell_or_collection(collection_mutation(ctype, collection)));
    }
};
