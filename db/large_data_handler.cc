/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/print.hh>
#include "db/query_context.hh"
#include "db/system_keyspace.hh"
#include "db/large_data_handler.hh"
#include "sstables/sstables.hh"

namespace db {

nop_large_data_handler::nop_large_data_handler()
    : large_data_handler(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(),
          std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()) {
    // Don't require start() to be called on nop large_data_handler.
    start();
}

future<> large_data_handler::maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size) {
    assert(running());
    if (partition_size > _partition_threshold_bytes) {
        ++_stats.partitions_bigger_than_threshold;
        return with_sem([&sst, &key, partition_size, this] {
            return record_large_partitions(sst, key, partition_size);
        });
    }
    return make_ready_future<>();
}

void large_data_handler::start() {
    _running = true;
}

future<> large_data_handler::stop() {
    if (!running()) {
        return make_ready_future<>();
    }
    _running = false;
    return _sem.wait(max_concurrency);
}

static logging::logger large_data_logger("large_data");

template <typename T> static std::string key_to_str(const T& key, const schema& s) {
    std::ostringstream oss;
    oss << key.with_schema(s);
    return oss.str();
}

template <typename... Args>
static future<> try_record(std::string_view large_table, const sstables::sstable& sst,  const sstables::key& partition_key, int64_t size,
        std::string_view desc, std::string_view extra_path, const std::vector<sstring> &extra_fields, Args&&... args) {
    sstring extra_fields_str;
    sstring extra_values;
    for (std::string_view field : extra_fields) {
        extra_fields_str += format(", {}", field);
        extra_values += ", ?";
    }
    const sstring req = format("INSERT INTO system.large_{}s (keyspace_name, table_name, sstable_name, {}_size, partition_key, compaction_time{}) VALUES (?, ?, ?, ?, ?, ?{}) USING TTL 2592000",
            large_table, large_table, extra_fields_str, extra_values);
    const schema &s = *sst.get_schema();
    auto ks_name = s.ks_name();
    auto cf_name = s.cf_name();
    const auto sstable_name = sst.get_filename();
    std::string pk_str = key_to_str(partition_key.to_partition_key(s), s);
    auto timestamp = db_clock::now();
    large_data_logger.warn("Writing large {} {}/{}: {}{} ({} bytes)", desc, ks_name, cf_name, pk_str, extra_path, size);
    return db::qctx->execute_cql(req, ks_name, cf_name, sstable_name, size, pk_str, timestamp, args...)
            .discard_result()
            .handle_exception([ks_name, cf_name, large_table, sstable_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to add a record to system.large_{}s: ks = {}, table = {}, sst = {} exception = {}",
                        large_table, ks_name, cf_name, sstable_name, ep);
            });
}

future<> cql_table_large_data_handler::record_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size) const {
    return try_record("partition", sst, key, int64_t(partition_size), "partition", "", {});
}

void cql_table_large_data_handler::log_too_many_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        uint64_t rows_count) const {
    const schema& s = *sst.get_schema();
    large_data_logger.warn("Writing a partition with too many rows [{}/{}:{}] ({} rows)",
                           s.ks_name(), s.cf_name(), partition_key.to_partition_key(s).with_schema(s),
                           rows_count);
}

future<> cql_table_large_data_handler::record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size) const {
    auto column_name = cdef.name_as_text();
    std::string_view cell_type = cdef.is_atomic() ? "cell" : "collection";
    static const std::vector<sstring> extra_fields{"clustering_key", "column_name"};
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        auto ck_str = key_to_str(*clustering_key, s);
        return try_record("cell", sst, partition_key, int64_t(cell_size), cell_type, format("{} {}", ck_str, column_name), extra_fields, ck_str, column_name);
    } else {
        return try_record("cell", sst, partition_key, int64_t(cell_size), cell_type, column_name, extra_fields, nullptr, column_name);
    }
}

future<> cql_table_large_data_handler::record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) const {
    static const std::vector<sstring> extra_fields{"clustering_key"};
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        std::string ck_str = key_to_str(*clustering_key, s);
        return try_record("row", sst, partition_key, int64_t(row_size), "row", ck_str, extra_fields,  ck_str);
    } else {
        return try_record("row", sst, partition_key, int64_t(row_size), "static row", "", extra_fields, nullptr);
    }
}

future<> cql_table_large_data_handler::delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const {
    const sstring req =
            format("DELETE FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?",
                    large_table_name);
    return db::qctx->execute_cql(req, s.ks_name(), s.cf_name(), sstable_name)
            .discard_result()
            .handle_exception([&s, sstable_name, large_table_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to drop entries from {}: ks = {}, table = {}, sst = {} exception = {}",
                        large_table_name, s.ks_name(), s.cf_name(), sstable_name, ep);
            });
}
}
