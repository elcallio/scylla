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

#pragma once

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "service/storage_proxy.hh"
#include "timestamp.hh"

class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;

class service_permit;

class mutation;
class partition_key;

namespace service {
    class query_state;
}

namespace cdc {

// cdc log table operation
enum class operation : int8_t {
    pre_image, update, row_delete, range_delete_start, range_delete_end, partition_delete
};

// cdc log data column operation
enum class column_op : int8_t {
    set, del, add,
};

seastar::future<> setup(service::storage_proxy& proxy, schema_ptr schema);

seastar::future<>
remove(const seastar::sstring& ks_name, const seastar::sstring& table_name);

seastar::sstring log_name(const seastar::sstring& table_name);

seastar::sstring desc_name(const seastar::sstring& table_name);

seastar::future<std::vector<mutation>>apply(
        service::storage_proxy& proxy,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service::query_state& qs, 
        const cql3::query_options&,
        std::vector<mutation> mutations);

} // namespace cdc
