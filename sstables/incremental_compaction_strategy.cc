/*
 * Copyright (C) 2019 ScyllaDB
 *
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "sstables.hh"
#include "sstable_set.hh"
#include "database.hh"
#include "compaction.hh"
#include "compaction_manager.hh"
#include "incremental_compaction_strategy.hh"
#include "incremental_backlog_tracker.hh"
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>

namespace sstables {

extern logging::logger clogger;

uint64_t incremental_compaction_strategy::avg_size(std::vector<sstables::sstable_run>& runs) const {
    uint64_t n = 0;

    if (runs.empty()) {
        return 0;
    }
    for (auto& r : runs) {
        n += r.data_size();
    }
    return n / runs.size();
}

bool incremental_compaction_strategy::is_bucket_interesting(const std::vector<sstables::sstable_run>& bucket, size_t min_threshold) const {
    return bucket.size() >= min_threshold;
}

bool incremental_compaction_strategy::is_any_bucket_interesting(const std::vector<std::vector<sstables::sstable_run>>& buckets, size_t min_threshold) const {
    return boost::algorithm::any_of(buckets, [&] (const std::vector<sstables::sstable_run>& bucket) {
        return this->is_bucket_interesting(bucket, min_threshold);
    });
}

std::vector<sstable_run_and_length>
incremental_compaction_strategy::create_run_and_length_pairs(const std::vector<sstables::sstable_run>& runs) const {

    std::vector<sstable_run_and_length> run_length_pairs;
    run_length_pairs.reserve(runs.size());

    for(auto& r : runs) {
        assert(r.data_size() != 0);
        run_length_pairs.emplace_back(r, r.data_size());
    }

    return run_length_pairs;
}

std::vector<std::vector<sstables::sstable_run>>
incremental_compaction_strategy::get_buckets(const std::vector<sstables::sstable_run>& runs) const {
    auto sorted_runs = create_run_and_length_pairs(runs);

    std::sort(sorted_runs.begin(), sorted_runs.end(), [] (sstable_run_and_length& i, sstable_run_and_length& j) {
        return i.second < j.second;
    });

    std::map<size_t, std::vector<sstables::sstable_run>> buckets;

    for (auto& pair : sorted_runs) {
        bool found = false;
        size_t size = pair.second;

        for (auto it = buckets.begin(); it != buckets.end(); it++) {
            size_t old_average_size = it->first;

            if ((size >= (old_average_size * _options.bucket_low) && size < (old_average_size * _options.bucket_high)) ||
                    (size < _options.min_sstable_size && old_average_size < _options.min_sstable_size)) {
                auto bucket = std::move(it->second);
                size_t total_size = bucket.size() * old_average_size;
                size_t new_average_size = (total_size + size) / (bucket.size() + 1);

                bucket.push_back(pair.first);
                buckets.erase(it);
                buckets.insert({ new_average_size, std::move(bucket) });

                found = true;
                break;
            }
        }

        // no similar bucket found; put it in a new one
        if (!found) {
            std::vector<sstables::sstable_run> new_bucket;
            new_bucket.push_back(std::move(pair.first));
            buckets.insert({ size, std::move(new_bucket) });
        }
    }

    std::vector<std::vector<sstables::sstable_run>> bucket_list;
    bucket_list.reserve(buckets.size());

    for (auto& entry : buckets) {
        bucket_list.push_back(std::move(entry.second));
    }

    return bucket_list;
}

std::vector<sstables::sstable_run>
incremental_compaction_strategy::most_interesting_bucket(std::vector<std::vector<sstables::sstable_run>> buckets,
        size_t min_threshold, size_t max_threshold)
{
    std::vector<sstable_run_bucket_and_length> interesting_buckets;
    interesting_buckets.reserve(buckets.size());

    for (auto& bucket : buckets) {
        bucket.resize(std::min(bucket.size(), max_threshold));
        if (is_bucket_interesting(bucket, min_threshold)) {
            auto avg = avg_size(bucket);
            interesting_buckets.push_back({ std::move(bucket), avg });
        }
    }

    if (interesting_buckets.empty()) {
        return std::vector<sstables::sstable_run>();
    }
    auto& min = *std::min_element(interesting_buckets.begin(), interesting_buckets.end(),
                    [] (sstable_run_bucket_and_length& i, sstable_run_bucket_and_length& j) {
        return i.second < j.second;
    });
    return std::move(min.first);
}

compaction_descriptor
incremental_compaction_strategy::get_sstables_for_compaction(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    // make local copies so they can't be changed out from under us mid-method
    size_t min_threshold = cf.min_compaction_threshold();
    size_t max_threshold = cf.schema()->max_compaction_threshold();

    auto buckets = get_buckets(cf.get_sstable_set().select_sstable_runs(candidates));

    auto get_all = [](std::vector<sstables::sstable_run> runs) mutable {
        return boost::accumulate(runs, std::vector<shared_sstable>(), [&] (std::vector<shared_sstable>&& v, const sstable_run& run) {
            v.insert(v.end(), run.all().begin(), run.all().end());
            return std::move(v);
        });
    };

    if (is_any_bucket_interesting(buckets, min_threshold)) {
        std::vector<sstables::sstable_run> most_interesting = most_interesting_bucket(std::move(buckets), min_threshold, max_threshold);
        return sstables::compaction_descriptor(get_all(std::move(most_interesting)), cf.get_sstable_set(), service::get_local_compaction_priority(), 0, _fragment_size);
    }
    // If we are not enforcing min_threshold explicitly, try any pair of sstable runs in the same tier.
    if (!cf.compaction_enforce_min_threshold() && is_any_bucket_interesting(buckets, 2)) {
        std::vector<sstables::sstable_run> most_interesting = most_interesting_bucket(std::move(buckets), 2, max_threshold);
        return sstables::compaction_descriptor(get_all(std::move(most_interesting)), cf.get_sstable_set(), service::get_local_compaction_priority(), 0, _fragment_size);
    }

    // The SAG behavior is only triggered once we're done with all the pending same-tier compaction to
    // increase overall efficiency.
    if (_space_amplification_goal) {
        if (buckets.size() < 2) {
            return sstables::compaction_descriptor();
        }
        // Let S0 be the size of largest tier
        // Let S1 be the size of second-largest tier,
        // SA will be (S0 + S1) / S0

        // Don't try SAG if there's an ongoing compaction, because if largest tier is being compacted,
        // SA would be calculated incorrectly, which may result in an unneeded cross-tier compaction.
        if (cf.get_compaction_manager().has_table_ongoing_compaction(&cf)) {
            return sstables::compaction_descriptor();
        }

        auto find_two_largest_tiers = [this] (std::vector<size_bucket_t>&& buckets) -> std::tuple<size_bucket_t, size_bucket_t> {
            std::partial_sort(buckets.begin(), buckets.begin()+2, buckets.end(), [this] (size_bucket_t& i, size_bucket_t& j) {
                return avg_size(i) > avg_size(j); // descending order
            });
            return { std::move(buckets[0]), std::move(buckets[1]) };
        };

        auto total_size = [] (const size_bucket_t& bucket) -> uint64_t {
            return boost::accumulate(bucket | boost::adaptors::transformed(std::mem_fn(&sstable_run::data_size)), uint64_t(0));
        };

        auto [s0, s1] = find_two_largest_tiers(std::move(buckets));
        uint64_t s0_size = total_size(s0), s1_size = total_size(s1);
        double space_amplification = double(s0_size + s1_size) / s0_size;

        if (space_amplification > _space_amplification_goal) {
            clogger.debug("ICS: doing cross-tier compaction of two largest tiers, to reduce SA {} to below SAG {}",
                          space_amplification, *_space_amplification_goal);
            // Aims at reducing space amplification, to below SAG, by compacting together the two largest tiers
            std::vector<sstables::sstable_run> cross_tier_input = std::move(s0);
            cross_tier_input.reserve(cross_tier_input.size() + s1.size());
            std::move(s1.begin(), s1.end(), std::back_inserter(cross_tier_input));

            return sstables::compaction_descriptor(get_all(std::move(cross_tier_input)), cf.get_sstable_set(),
                                                   service::get_local_compaction_priority(), 0, _fragment_size);
        }
    }

    return sstables::compaction_descriptor();
}

compaction_descriptor
incremental_compaction_strategy::get_major_compaction_job(column_family& cf, std::vector<sstables::shared_sstable> candidates) {
    if (candidates.empty()) {
        return compaction_descriptor();
    }
    return compaction_descriptor(std::move(candidates), cf.get_sstable_set(), service::get_local_compaction_priority(), 0, _fragment_size);
}

int64_t incremental_compaction_strategy::estimated_pending_compactions(column_family& cf) const {
    size_t min_threshold = cf.schema()->min_compaction_threshold();
    size_t max_threshold = cf.schema()->max_compaction_threshold();
    std::vector<sstables::shared_sstable> sstables;
    int64_t n = 0;

    sstables.reserve(cf.sstables_count());
    for (auto& entry : *cf.get_sstables()) {
        sstables.push_back(entry);
    }

    for (auto& bucket : get_buckets(cf.get_sstable_set().select_sstable_runs(sstables))) {
        if (bucket.size() >= min_threshold) {
            n += (bucket.size() + max_threshold - 1) / max_threshold;
        }
    }
    return n;
}

incremental_compaction_strategy::incremental_compaction_strategy(const std::map<sstring, sstring>& options)
    : compaction_strategy_impl(options)
    , _options(options)
    , _backlog_tracker(std::make_unique<incremental_backlog_tracker>())
{
    using namespace cql3::statements;
    auto option_value = compaction_strategy_impl::get_value(options, FRAGMENT_SIZE_OPTION);
    auto fragment_size_in_mb = property_definitions::to_int(FRAGMENT_SIZE_OPTION, option_value, DEFAULT_MAX_FRAGMENT_SIZE_IN_MB);

    if (fragment_size_in_mb < 100) {
        clogger.warn("SStable size of {}MB is configured. The value may lead to sstable run having an substatial amount of fragments, leading to undesired overhead.",
                     fragment_size_in_mb);
    }
    _fragment_size = fragment_size_in_mb*1024*1024;

    if ((option_value = compaction_strategy_impl::get_value(options, SPACE_AMPLIFICATION_GOAL_OPTION))) {
        _space_amplification_goal = property_definitions::to_double(SPACE_AMPLIFICATION_GOAL_OPTION, option_value, 0.0);
        if (_space_amplification_goal <= 1.0) {
            throw exceptions::configuration_exception("Incremental Compaction Strategy - " \
                "value of space_amplification_goal must be greater than 1.");
        }
    }
}

}
