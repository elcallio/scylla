/*
 * Copyright (C) 2017 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


// Glue logic for writing memtables to sstables

#pragma once

#include "memtable.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/progress_monitor.hh"
#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/shared_ptr.hh>

namespace sstables {
class sstables_manager;
class sstable_writer_config;
}

future<>
write_memtable_to_sstable(flat_mutation_reader reader,
        memtable& mt, sstables::shared_sstable sst,
        sstables::write_monitor& monitor,
        sstables::sstable_writer_config& cfg,
        const io_priority_class& pc);

future<>
write_memtable_to_sstable(memtable& mt,
        sstables::shared_sstable sst,
        sstables::write_monitor& mon,
        sstables::sstable_writer_config& cfg,
        const io_priority_class& pc = default_priority_class());

future<>
write_memtable_to_sstable(memtable& mt,
        sstables::shared_sstable sst,
        sstables::sstable_writer_config cfg);
