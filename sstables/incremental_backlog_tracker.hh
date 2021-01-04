/*
 * Copyright (C) 2019 ScyllaDB
 *
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once
#include "sstables/compaction_backlog_manager.hh"
#include <cmath>
#include <ctgmath>
#include <boost/range/adaptor/map.hpp>

// The only difference to size tiered backlog tracker is that it will calculate
// backlog contribution using total bytes of each sstable run instead of total
// bytes of an individual sstable object.
class incremental_backlog_tracker final : public compaction_backlog_tracker::impl {
    std::unordered_map<utils::UUID, int64_t> _total_bytes_per_run;
    int64_t _total_bytes = 0;

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component partial_backlog(const compaction_backlog_tracker::ongoing_writes& ongoing_writes) const {
        inflight_component in;
        for (auto& swp :  ongoing_writes) {
            auto written = swp.second->written();
            if (written > 0) {
                in.total_bytes += written;
                in.contribution += written * log4(written);
            }
        }
        return in;
    }

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const {
        inflight_component in;
        for (auto& crp : ongoing_compactions) {
            auto compacted = crp.second->compacted();
            in.total_bytes += compacted;
            in.contribution += compacted * log4((crp.first->data_size()));
        }
        return in;
    }
    double log4(double x) const {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    int64_t incremental_sstables_backlog_contribution() const {
        double sstables_backlog_contribution = 0.0f;
        for (auto& total_bytes : _total_bytes_per_run | boost::adaptors::map_values) {
            sstables_backlog_contribution += total_bytes * log4(total_bytes);
        }
        return sstables_backlog_contribution;
    }
public:
    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        inflight_component partial = partial_backlog(ow);
        inflight_component compacted = compacted_backlog(oc);

        auto total_bytes = _total_bytes + partial.total_bytes - compacted.total_bytes;
        if ((total_bytes <= 0)) {
            return 0;
        }
        auto sstables_contribution = incremental_sstables_backlog_contribution() + partial.contribution - compacted.contribution;
        auto b = (total_bytes * log4(total_bytes)) - sstables_contribution;
        return b > 0 ? b : 0;
    }

    virtual void add_sstable(sstables::shared_sstable sst)  override {
        if (sst->data_size() > 0) {
            _total_bytes_per_run[sst->run_identifier()] += sst->data_size();
            _total_bytes += sst->data_size();
        }
    }

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    virtual void remove_sstable(sstables::shared_sstable sst)  override {
        if (sst->data_size() > 0) {
            auto run_identifier = sst->run_identifier();
            _total_bytes_per_run[run_identifier] -= sst->data_size();
            if (!_total_bytes_per_run[run_identifier]) {
                _total_bytes_per_run.erase(run_identifier);
            }
            _total_bytes -= sst->data_size();
        }
    }
    int64_t total_bytes() const {
        return _total_bytes;
    }
};
