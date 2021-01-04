/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/noncopyable_function.hh>

#include "feed_writers.hh"

namespace mutation_writer {

// Given a producer that may contain data for all shards, consume it in a per-shard
// manner. This is useful, for instance, in the resharding process where a user changes
// the amount of CPU assigned to Scylla and we have to rewrite the SSTables to their new
// owners.
future<> segregate_by_shard(flat_mutation_reader producer, reader_consumer consumer);

} // namespace mutation_writer
