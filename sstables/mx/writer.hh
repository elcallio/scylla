/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "sstables/writer_impl.hh"
#include "sstables/types.hh"
#include "encoding_stats.hh"

namespace sstables {
namespace mc {

std::unique_ptr<sstable_writer::writer_impl> make_writer(sstable& sst,
    const schema& s,
    uint64_t estimated_partitions,
    const sstable_writer_config& cfg,
    encoding_stats enc_stats,
    const io_priority_class& pc,
    shard_id shard);

}
}
