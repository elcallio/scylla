/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "mutation_reader.hh"
#include "db/system_keyspace.hh"

namespace db {

namespace size_estimates {

struct token_range {
    bytes start;
    bytes end;
};

class size_estimates_mutation_reader final : public flat_mutation_reader::impl {
    const dht::partition_range* _prange;
    const query::partition_slice& _slice;
    using ks_range = std::vector<sstring>;
    std::optional<ks_range> _keyspaces;
    ks_range::const_iterator _current_partition;
    streamed_mutation::forwarding _fwd;
    flat_mutation_reader_opt _partition_reader;
public:
    size_estimates_mutation_reader(schema_ptr, reader_permit, const dht::partition_range&, const query::partition_slice&, streamed_mutation::forwarding);

    virtual future<> fill_buffer(db::timeout_clock::time_point) override;
    virtual void next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override;
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override;
private:
    future<> get_next_partition();

    std::vector<db::system_keyspace::range_estimates>
    estimates_for_current_keyspace(const database&, std::vector<token_range> local_ranges) const;
};

struct virtual_reader {
    flat_mutation_reader operator()(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        return make_flat_mutation_reader<size_estimates_mutation_reader>(std::move(schema), std::move(permit), range, slice, fwd);
    }
};

future<std::vector<token_range>> test_get_local_ranges(database& db);

} // namespace size_estimates

} // namespace db
