/*
 * Copyright (C) 2019 ScyllaDB
 *
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "compaction_strategy_impl.hh"
#include "compaction.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "size_tiered_compaction_strategy.hh"

namespace sstables {

class incremental_compaction_strategy_options {
    static constexpr uint64_t DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    static constexpr double DEFAULT_BUCKET_LOW = 0.5;
    static constexpr double DEFAULT_BUCKET_HIGH = 1.5;

    const sstring MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    const sstring BUCKET_LOW_KEY = "bucket_low";
    const sstring BUCKET_HIGH_KEY = "bucket_high";

    uint64_t min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
    double bucket_low = DEFAULT_BUCKET_LOW;
    double bucket_high = DEFAULT_BUCKET_HIGH;
public:
    incremental_compaction_strategy_options(const std::map<sstring, sstring>& options) {
        using namespace cql3::statements;

        auto tmp_value = compaction_strategy_impl::get_value(options, MIN_SSTABLE_SIZE_KEY);
        min_sstable_size = property_definitions::to_long(MIN_SSTABLE_SIZE_KEY, tmp_value, DEFAULT_MIN_SSTABLE_SIZE);

        tmp_value = compaction_strategy_impl::get_value(options, BUCKET_LOW_KEY);
        bucket_low = property_definitions::to_double(BUCKET_LOW_KEY, tmp_value, DEFAULT_BUCKET_LOW);

        tmp_value = compaction_strategy_impl::get_value(options, BUCKET_HIGH_KEY);
        bucket_high = property_definitions::to_double(BUCKET_HIGH_KEY, tmp_value, DEFAULT_BUCKET_HIGH);
    }

    incremental_compaction_strategy_options() {
        min_sstable_size = DEFAULT_MIN_SSTABLE_SIZE;
        bucket_low = DEFAULT_BUCKET_LOW;
        bucket_high = DEFAULT_BUCKET_HIGH;
    }

    friend class incremental_compaction_strategy;
};

using sstable_run_and_length = std::pair<sstables::sstable_run, uint64_t>;
using sstable_run_bucket_and_length = std::pair<std::vector<sstables::sstable_run>, uint64_t>;

class incremental_compaction_strategy : public compaction_strategy_impl {
    incremental_compaction_strategy_options _options;
    compaction_backlog_tracker _backlog_tracker;

    using size_bucket_t = std::vector<sstables::sstable_run>;

    static constexpr int32_t DEFAULT_MAX_FRAGMENT_SIZE_IN_MB = 1000;
    const sstring FRAGMENT_SIZE_OPTION = "sstable_size_in_mb";
    size_t _fragment_size = DEFAULT_MAX_FRAGMENT_SIZE_IN_MB*1024*1024;

    const sstring SPACE_AMPLIFICATION_GOAL_OPTION = "space_amplification_goal";
    std::optional<double> _space_amplification_goal;

    std::vector<sstable_run_and_length> create_run_and_length_pairs(const std::vector<sstables::sstable_run>& runs) const;

    std::vector<std::vector<sstables::sstable_run>> get_buckets(const std::vector<sstables::sstable_run>& runs) const;

    std::vector<sstables::sstable_run>
    most_interesting_bucket(std::vector<std::vector<sstables::sstable_run>> buckets, size_t min_threshold, size_t max_threshold);

    uint64_t avg_size(std::vector<sstables::sstable_run>& runs) const;

    bool is_bucket_interesting(const std::vector<sstables::sstable_run>& bucket, size_t min_threshold) const;

    bool is_any_bucket_interesting(const std::vector<std::vector<sstables::sstable_run>>& buckets, size_t min_threshold) const;
public:
    incremental_compaction_strategy() = default;

    incremental_compaction_strategy(const std::map<sstring, sstring>& options);

    virtual compaction_descriptor get_sstables_for_compaction(column_family& cf, std::vector<sstables::shared_sstable> candidates) override;

    virtual compaction_descriptor get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates) override;

    virtual int64_t estimated_pending_compactions(column_family& cf) const override;

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::incremental;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }

    virtual std::unique_ptr<sstable_set_impl> make_sstable_set(schema_ptr schema) const override;
};

}
