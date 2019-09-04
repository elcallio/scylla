/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <utility>

#include "cdc/cdc.hh"
#include "database.hh"
#include "db/config.hh"
#include "dht/murmur3_partitioner.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/storage_service.hh"

using seastar::sstring;

namespace cdc {

sstring log_name(const sstring& table_name) {
    static constexpr const auto cdc_log_suffix = "_scylla_cdc_log";
    return table_name + cdc_log_suffix;
}

sstring desc_name(const sstring& table_name) {
    static constexpr const auto cdc_desc_suffix = "_scylla_cdc_desc";
    return table_name + cdc_desc_suffix;
}

static future<> remove_log(const sstring& ks_name, const sstring& table_name) {
    auto& mm = service::get_local_migration_manager();
    return mm.announce_column_family_drop(ks_name, log_name(table_name), false);
}

static future<> remove_desc(const sstring& ks_name, const sstring& table_name) {
    auto& mm = service::get_local_migration_manager();
    return mm.announce_column_family_drop(ks_name, desc_name(table_name), false);
}

future<> remove(const sstring& ks_name, const sstring& table_name) {
    return when_all(remove_log(ks_name, table_name), remove_desc(ks_name, table_name)).discard_result();
}

static future<> setup_log(const schema& s) {
    schema_builder b(s.ks_name(), log_name(s.cf_name()));
    b.set_default_time_to_live(gc_clock::duration{86400}); // 24h
    b.set_comment(sprint("CDC log for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("stream_id", uuid_type, column_kind::partition_key);
    b.with_column("time", timeuuid_type, column_kind::clustering_key);
    b.with_column("batch_seq_no", int32_type, column_kind::clustering_key);
    b.with_column("operation", int32_type);
    b.with_column("ttl", long_type);
    auto add_columns = [&] (const auto& columns) {
        for (const auto& column : columns) {
            b.with_column("_" + column.name(), column.type);
        }
    };
    add_columns(s.partition_key_columns());
    add_columns(s.clustering_key_columns());
    add_columns(s.static_columns());
    add_columns(s.regular_columns());
    auto& mm = service::get_local_migration_manager();
    return mm.announce_new_column_family(b.build(), false);
}

static future<> setup_desc(const schema& s) {
    schema_builder b(s.ks_name(), desc_name(s.cf_name()));
    b.set_comment(sprint("CDC description for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("node_ip", inet_addr_type, column_kind::partition_key);
    b.with_column("shard_id", int32_type, column_kind::partition_key);
    b.with_column("created_at", timestamp_type, column_kind::clustering_key);
    b.with_column("stream_id", uuid_type);
    auto& mm = service::get_local_migration_manager();
    return mm.announce_new_column_family(b.build(), false);
}

template <typename Func>
static void for_each_endpoint(Func&& f) {
    auto& topology = service::get_local_storage_service().get_token_metadata().get_topology();
    for (const auto& dc : topology.get_datacenter_endpoints()) {
        for (const auto& endpoint : dc.second) {
            f(endpoint);
        }
    }
}

template <typename Func>
static void for_each_shard(const gms::inet_address& endpoint, Func&& f) {
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    const unsigned int shard_count = snitch_ptr->get_shard_count(endpoint);
    for (unsigned int shard_id = 0; shard_id < shard_count; ++shard_id) {
        f(shard_id, shard_count);
    }
}

static future<>
populate_desc(service::storage_proxy& proxy, const schema& s) {
    auto& db = proxy.get_db().local();
    auto desc_schema =
        db.find_schema(s.ks_name(), desc_name(s.cf_name()));
    auto log_schema =
        db.find_schema(s.ks_name(), log_name(s.cf_name()));
    auto& token_metadata =
        service::get_local_storage_service().get_token_metadata();
    auto& replication_strategy =
        db.find_keyspace(s.ks_name()).get_replication_strategy();
    // The assumption here is that all nodes in the cluster are using the
    // same murmur3_partitioner_ignore_msb_bits. If it's not true, we will
    // have to gossip it around together with shard_count.
    auto ignore_msb_bits =
        db.get_config().murmur3_partitioner_ignore_msb_bits();

    auto belongs_to = [&](const gms::inet_address& endpoint,
                          const unsigned int shard_id,
                          const int shard_count,
                          const utils::UUID& stream_id) {
        const auto log_pk = partition_key::from_singular(*log_schema,
                                                         data_value(stream_id));
        const auto token =
            dht::global_partitioner().decorate_key(*log_schema, log_pk).token();
        auto&& eps =
            replication_strategy.calculate_natural_endpoints(token, token_metadata);
        if (eps.empty() || eps[0] != endpoint) {
            return false;
        }
        const auto owning_shard_id = dht::murmur3_partitioner::shard_of(
                token, shard_count, ignore_msb_bits);
        return owning_shard_id == shard_id;
    };

    std::vector<mutation> mutations;
    const auto ts = api::new_timestamp();
    const auto ck = clustering_key::from_single_value(
            *desc_schema, timestamp_type->decompose(ts));
    auto cdef = desc_schema->get_column_definition(to_bytes("stream_id"));

    for_each_endpoint([&] (const gms::inet_address& endpoint) {
        const auto decomposed_ip = inet_addr_type->decompose(endpoint.addr());

        for_each_shard(endpoint, [&] (unsigned int shard_id, int shard_count) {
            const auto pk = partition_key::from_exploded(
                    *desc_schema, { decomposed_ip, int32_type->decompose(static_cast<int>(shard_id)) });
            mutations.emplace_back(desc_schema, pk);

            auto stream_id = utils::make_random_uuid();
            while (!belongs_to(endpoint, shard_id, shard_count, stream_id)) {
                stream_id = utils::make_random_uuid();
            }
            auto value = atomic_cell::make_live(*uuid_type,
                                                ts,
                                                uuid_type->decompose(stream_id));
            mutations.back().set_cell(ck, *cdef, std::move(value));
        });
    });
    return proxy.mutate(std::move(mutations),
                        db::consistency_level::QUORUM,
                        db::no_timeout,
                        nullptr,
                        empty_service_permit());
}

future<> setup(service::storage_proxy& proxy, schema_ptr s) {
    return setup_log(*s).then([&proxy, s = std::move(s)] () mutable {
        return setup_desc(*s).then_wrapped([&proxy, s = std::move(s)] (future<> f) mutable {
            if (f.failed()) {
                return remove_log(s->ks_name(), s->cf_name());
            }
            return populate_desc(proxy, *s).handle_exception([s = std::move(s)] (std::exception_ptr ep) mutable {
                return remove_desc(s->ks_name(), s->cf_name()).then([s = std::move(s)] {
                    return remove_log(s->ks_name(), s->cf_name());
                }).then([ep = std::move(ep)] {
                    return make_exception_future<>(std::move(ep));
                });;
            });
        });
    });
}

} // namespace cdc
