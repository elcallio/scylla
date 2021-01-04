/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "database_fwd.hh"
#include "sstables/sstables.hh"
#include "db/view/view_updating_consumer.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

namespace db::view {

class view_update_generator {
public:
    static constexpr size_t registration_queue_size = 5;

private:
    database& _db;
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    seastar::condition_variable _pending_sstables;
    named_semaphore _registration_sem{registration_queue_size, named_semaphore_exception_factory{"view update generator"}};
    struct sstable_with_table {
        sstables::shared_sstable sst;
        lw_shared_ptr<table> t;
        sstable_with_table(sstables::shared_sstable sst, lw_shared_ptr<table> t) : sst(std::move(sst)), t(std::move(t)) { }
    };
    std::unordered_map<lw_shared_ptr<table>, std::vector<sstables::shared_sstable>> _sstables_with_tables;
    std::unordered_map<lw_shared_ptr<table>, std::vector<sstables::shared_sstable>> _sstables_to_move;
    metrics::metric_groups _metrics;
public:
    view_update_generator(database& db) : _db(db) {
        setup_metrics();
    }

    future<> start();
    future<> stop();
    future<> register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<table> table);

    ssize_t available_register_units() const { return _registration_sem.available_units(); }
private:
    bool should_throttle() const;
    void setup_metrics();
};

}
