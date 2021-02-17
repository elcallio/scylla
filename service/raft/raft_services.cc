/*
 * Copyright (C) 2020 ScyllaDB
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
#include "service/raft/raft_services.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/raft/schema_raft_state_machine.hh"
#include "service/raft/raft_gossip_failure_detector.hh"

#include "raft/raft.hh"
#include "message/messaging_service.hh"
#include "cql3/query_processor.hh"
#include "gms/gossiper.hh"

#include <seastar/core/smp.hh>

raft_services::raft_services(netw::messaging_service& ms, gms::gossiper& gs, cql3::query_processor& qp)
    : _ms(ms), _gossiper(gs), _qp(qp), _fd(make_shared<raft_gossip_failure_detector>(gs, *this))
{}

void raft_services::init_rpc_verbs() {
    auto handle_raft_rpc = [this] (
            const rpc::client_info& cinfo,
            uint64_t group_id, raft::server_id from, raft::server_id dst, auto handler) {
        return container().invoke_on(shard_for_group(group_id),
                [addr = netw::messaging_service::get_source(cinfo).addr, from, dst, handler] (raft_services& self) mutable {
            // Update the address mappings for the rpc module
            // in case the sender is encountered for the first time
            auto& rpc = self.get_rpc(dst);
            self.update_address_mapping(from, std::move(addr));
            // Execute the actual message handling code
            return handler(rpc);
        });
    };

    _ms.register_raft_send_snapshot([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            uint64_t group_id, raft::server_id from, raft::server_id dst, raft::install_snapshot snp) mutable {
        return handle_raft_rpc(cinfo, group_id, from, dst, [from, snp = std::move(snp)] (raft_rpc& rpc) mutable {
            return rpc.apply_snapshot(std::move(from), std::move(snp));
        });
    });

    _ms.register_raft_append_entries([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
           uint64_t group_id, raft::server_id from, raft::server_id dst, raft::append_request append_request) mutable {
        return handle_raft_rpc(cinfo, group_id, from, dst, [from, append_request = std::move(append_request)] (raft_rpc& rpc) mutable {
            rpc.append_entries(std::move(from), std::move(append_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_append_entries_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            uint64_t group_id, raft::server_id from, raft::server_id dst, raft::append_reply reply) mutable {
        return handle_raft_rpc(cinfo, group_id, from, dst, [from, reply = std::move(reply)] (raft_rpc& rpc) mutable {
            rpc.append_entries_reply(std::move(from), std::move(reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_request([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            uint64_t group_id, raft::server_id from, raft::server_id dst, raft::vote_request vote_request) mutable {
        return handle_raft_rpc(cinfo, group_id, from, dst, [from, vote_request] (raft_rpc& rpc) mutable {
            rpc.request_vote(std::move(from), std::move(vote_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            uint64_t group_id, raft::server_id from, raft::server_id dst, raft::vote_reply vote_reply) mutable {
        return handle_raft_rpc(cinfo, group_id, from, dst, [from, vote_reply] (raft_rpc& rpc) mutable {
            rpc.request_vote_reply(std::move(from), std::move(vote_reply));
            return make_ready_future<>();
        });
    });
}

future<> raft_services::uninit_rpc_verbs() {
    return when_all_succeed(
        _ms.unregister_raft_send_snapshot(),
        _ms.unregister_raft_append_entries(),
        _ms.unregister_raft_append_entries_reply(),
        _ms.unregister_raft_vote_request(),
        _ms.unregister_raft_vote_reply()
    ).discard_result();
}

void raft_services::init() {
    init_rpc_verbs();
    // schema raft server instance always resides on shard 0
    if (this_shard_id() == 0) {
        // FIXME: Server id will change each time scylla server restarts,
        // need to persist it or find a deterministic way to compute!
        raft::server_id id = {.id = utils::make_random_uuid()};
        add_server(id, create_schema_server(id));
    }
}

seastar::future<> raft_services::uninit() {
    return uninit_rpc_verbs();
}

raft_rpc& raft_services::get_rpc(raft::server_id id) {
    auto it = _servers.find(id);
    if (it == _servers.end()) {
        throw std::runtime_error(format("No raft server found with id = {}", id));
    }
    return *it->second.second;
}

raft_services::create_server_result raft_services::create_schema_server(raft::server_id id) {
    auto rpc = std::make_unique<raft_rpc>(_ms, *this, schema_raft_state_machine::group_id, id);
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(_qp, schema_raft_state_machine::group_id);
    auto state_machine = std::make_unique<schema_raft_state_machine>();

    return std::pair(raft::create_server(id,
            std::move(rpc),
            std::move(state_machine),
            std::move(storage),
            _fd,
            raft::server::configuration()), // use default raft server configuration
        &rpc_ref);
}

void raft_services::add_server(raft::server_id id, create_server_result srv) {
    _servers.emplace(std::pair(id, std::move(srv)));
}

unsigned raft_services::shard_for_group(uint64_t group_id) const {
    if (group_id == schema_raft_state_machine::group_id) {
        return 0; // schema raft server is always owned by shard 0
    }
    // We haven't settled yet on how to organize and manage (group_id -> shard_id) mapping
    throw std::runtime_error(format("Could not map raft group id {} to a corresponding shard id", group_id));
}

gms::inet_address raft_services::get_inet_address(raft::server_id id) const {
    auto it = _server_addresses.find(id);
    if (it == _server_addresses.end()) {
        throw std::runtime_error(format("Destination raft server not found with id {}", id));
    }
    return it->second;
}

void raft_services::update_address_mapping(raft::server_id id, gms::inet_address addr) {
    auto addr_it = _server_addresses.find(id);
    if (addr_it == _server_addresses.end()) {
        _server_addresses[id] = addr;
        return;
    }
    if (addr_it->second != addr) {
        throw std::runtime_error(
            format("update_address_mapping: expected to get inet_address {} for raft server id {} (got {})",
                addr_it->second, id, addr));
    }
}

void raft_services::remove_address_mapping(raft::server_id id) {
    _server_addresses.erase(id);
}