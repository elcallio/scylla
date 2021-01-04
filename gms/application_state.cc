/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "gms/application_state.hh"
#include <seastar/core/sstring.hh>
#include <ostream>
#include <map>
#include "seastarx.hh"

namespace gms {

static const std::map<application_state, sstring> application_state_names = {
    {application_state::STATUS,                 "STATUS"},
    {application_state::LOAD,                   "LOAD"},
    {application_state::SCHEMA,                 "SCHEMA"},
    {application_state::DC,                     "DC"},
    {application_state::RACK,                   "RACK"},
    {application_state::RELEASE_VERSION,        "RELEASE_VERSION"},
    {application_state::REMOVAL_COORDINATOR,    "REMOVAL_COORDINATOR"},
    {application_state::INTERNAL_IP,            "INTERNAL_IP"},
    {application_state::RPC_ADDRESS,            "RPC_ADDRESS"},
    {application_state::SEVERITY,               "SEVERITY"},
    {application_state::NET_VERSION,            "NET_VERSION"},
    {application_state::HOST_ID,                "HOST_ID"},
    {application_state::TOKENS,                 "TOKENS"},
    {application_state::SUPPORTED_FEATURES,     "SUPPORTED_FEATURES"},
    {application_state::CACHE_HITRATES,         "CACHE_HITRATES"},
    {application_state::SCHEMA_TABLES_VERSION,  "SCHEMA_TABLES_VERSION"},
    {application_state::RPC_READY,              "RPC_READY"},
    {application_state::VIEW_BACKLOG,           "VIEW_BACKLOG"},
    {application_state::SHARD_COUNT,            "SHARD_COUNT"},
    {application_state::IGNORE_MSB_BITS,        "IGNOR_MSB_BITS"},
    {application_state::CDC_STREAMS_TIMESTAMP,  "CDC_STREAMS_TIMESTAMP"},
    {application_state::SNITCH_NAME,            "SNITCH_NAME"},
};

std::ostream& operator<<(std::ostream& os, const application_state& m) {
    auto it = application_state_names.find(m);
    if (it != application_state_names.end()) {
        os << application_state_names.at(m);
    } else {
        os << "UNKNOWN";
    }
    return os;
}

}

