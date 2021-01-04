/*
 * Copyright 2016 ScyllaDB
 */
/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include "priority_manager.hh"
#include <seastar/core/reactor.hh>

namespace service {
priority_manager& get_local_priority_manager() {
    static thread_local priority_manager pm = priority_manager();
    return pm;
}

priority_manager::priority_manager()
    : _commitlog_priority(engine().register_one_priority_class("commitlog", 1000))
    , _mt_flush_priority(engine().register_one_priority_class("memtable_flush", 1000))
    , _streaming_priority(engine().register_one_priority_class("streaming", 200))
    , _sstable_query_read(engine().register_one_priority_class("query", 1000))
    , _compaction_priority(engine().register_one_priority_class("compaction", 1000))
{}

}
