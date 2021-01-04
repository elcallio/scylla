/*
 * Copyright 2016 ScyllaDB
 */
/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/file.hh>
#include "seastarx.hh"
#include "qos/service_level_controller.hh"

namespace service {
class priority_manager {
    ::io_priority_class _commitlog_priority;
    ::io_priority_class _mt_flush_priority;
    ::io_priority_class _streaming_priority;
    ::io_priority_class _sstable_query_read;
    ::io_priority_class _compaction_priority;
    qos::service_level_controller* _sl_controller;
public:
    const ::io_priority_class&
    commitlog_priority() const {
        return _commitlog_priority;
    }

    const ::io_priority_class&
    memtable_flush_priority() const {
        return _mt_flush_priority;
    }

    const ::io_priority_class&
    streaming_priority() const {
        return _streaming_priority;
    }

    const ::io_priority_class&
    sstable_query_read_priority() const {
        if (_sl_controller) {
            io_priority_class* pc = _sl_controller->get_current_priority_class();
            if (pc) {
                return *pc;
            }
        }
        return _sstable_query_read;
    }

    const ::io_priority_class&
    compaction_priority() const {
        return _compaction_priority;
    }

    void set_service_level_controller(qos::service_level_controller* sl_controller) {
        _sl_controller = sl_controller;
    }
    priority_manager();
};

priority_manager& get_local_priority_manager();
const inline ::io_priority_class&
get_local_commitlog_priority() {
    return get_local_priority_manager().commitlog_priority();
}

const inline ::io_priority_class&
get_local_memtable_flush_priority() {
    return get_local_priority_manager().memtable_flush_priority();
}

const inline ::io_priority_class&
get_local_streaming_priority() {
    return get_local_priority_manager().streaming_priority();
}

const inline ::io_priority_class&
get_local_sstable_query_read_priority() {
    return get_local_priority_manager().sstable_query_read_priority();
}

const inline ::io_priority_class&
get_local_compaction_priority() {
    return get_local_priority_manager().compaction_priority();
}
}
