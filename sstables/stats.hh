/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cstdint>

namespace sstables {

class sstables_stats {
    static thread_local struct stats {
        uint64_t partition_writes = 0;
        uint64_t static_row_writes = 0;
        uint64_t row_writes = 0;
        uint64_t tombstone_writes = 0;
        uint64_t range_tombstone_writes = 0;
        uint64_t cell_writes = 0;
        uint64_t cell_tombstone_writes = 0;
        uint64_t single_partition_reads = 0;
        uint64_t range_partition_reads = 0;
        uint64_t sstable_partition_reads = 0;
        uint64_t partition_reads = 0;
        uint64_t partition_seeks = 0;
        uint64_t row_reads = 0;
        uint64_t capped_local_deletion_time = 0;
        uint64_t capped_tombstone_deletion_time = 0;
    } _shard_stats;

    stats& _stats = _shard_stats;

public:
    static const stats& get_shard_stats() {
        return _shard_stats;
    }

    inline void on_partition_write() {
        ++_stats.partition_writes;
    }

    inline void on_static_row_write() {
        ++_stats.static_row_writes;
    }

    inline void on_row_write() {
        ++_stats.row_writes;
    }

    inline void on_tombstone_write() {
        ++_stats.tombstone_writes;
    }

    inline void on_range_tombstone_write() {
        ++_stats.range_tombstone_writes;
    }

    inline void on_cell_write() {
        ++_stats.cell_writes;
    }

    inline void on_cell_tombstone_write() {
        ++_stats.cell_tombstone_writes;
    }

    inline void on_single_partition_read() {
        ++_stats.single_partition_reads;
    }

    inline void on_range_partition_read() {
        ++_stats.range_partition_reads;
    }

    inline void on_sstable_partition_read() {
        ++_stats.sstable_partition_reads;
    }

    inline void on_partition_read() {
        ++_stats.partition_reads;
    }

    inline void on_partition_seek() {
        ++_stats.partition_seeks;
    }

    inline void on_row_read() {
        ++_stats.row_reads;
    }

    inline void on_capped_local_deletion_time() {
        ++_stats.capped_local_deletion_time;
    }

    inline void on_capped_tombstone_deletion_time() {
        ++_stats.capped_tombstone_deletion_time;
    }
};

}
