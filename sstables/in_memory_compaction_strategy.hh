/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "compaction_strategy_impl.hh"
#include "compaction_strategy.hh"
#include "compaction.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

namespace sstables {

class in_memory_backlog_tracker final : public compaction_backlog_tracker::impl {
public:
    double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return 0.5;
    }

    void add_sstable(sstables::shared_sstable sst)  override {
    }

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    void remove_sstable(sstables::shared_sstable sst)  override {
    }
};

class in_memory_compaction_strategy_options {
public:
    in_memory_compaction_strategy_options(const std::map<sstring, sstring>& options) {}
    in_memory_compaction_strategy_options()  = default;
};

class in_memory_compaction_strategy : public compaction_strategy_impl {
    in_memory_compaction_strategy_options _options;
    compaction_backlog_tracker _backlog_tracker;

public:
    in_memory_compaction_strategy() = default;

    in_memory_compaction_strategy(const std::map<sstring, sstring>& options)
        : compaction_strategy_impl(options)
        , _options(options)
        , _backlog_tracker(std::make_unique<in_memory_backlog_tracker>())
    {}

    explicit in_memory_compaction_strategy(const in_memory_compaction_strategy_options& options)
    : _options(options)
    , _backlog_tracker(std::make_unique<in_memory_backlog_tracker>())
    {}

    compaction_descriptor get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) override;

    int64_t estimated_pending_compactions(column_family& cf) const override;

    compaction_strategy_type type() const override {
        return compaction_strategy_type::in_memory;
    }

    compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
};

inline compaction_descriptor
in_memory_compaction_strategy::get_sstables_for_compaction(column_family& cfs, std::vector<sstables::shared_sstable> candidates) {
    // compact everything into one sstable
    if (candidates.size() > 1) {
        return sstables::compaction_descriptor(std::move(candidates), cfs.get_sstable_set(), service::get_local_compaction_priority());
    }
    return sstables::compaction_descriptor();
}

inline int64_t in_memory_compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    return cf.sstables_count() > 1;
}
}
