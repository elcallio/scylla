/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once
#include "utils/UUID_gen.hh"
#include "frozen_mutation.hh"

namespace service {

namespace paxos {

// Proposal represents replica's value associated with a given ballot. The origin uses the term
// "commit" for this object, however, Scylla follows the terminology as set by Paxos Made Simple
// paper.
// Each replica persists the proposals it receives in the system.paxos table. A proposal may be
// new, accepted by a replica, or accepted by a majority. When a proposal is accepted by majority it
// is considered "chosen" by Paxos, and we call such a proposal "decision". A decision is
// saved in the paxos table in an own column and applied to the base table during "learn" phase of
// the protocol. After a decision is applied it is considered "committed".
class proposal {
public:
    // The ballot for the update.
    utils::UUID ballot;
    // The mutation representing the update that is being applied.
    frozen_mutation update;

    proposal(utils::UUID ballot_arg, frozen_mutation update_arg)
        : ballot(ballot_arg)
        , update(std::move(update_arg)) {}
};

// Proposals are ordered by their ballot's timestamp.
// A proposer uses it to find the newest proposal accepted
// by some replica among the responses to its own one.
inline bool operator<(const proposal& lhs, const proposal& rhs) {
    return lhs.ballot.timestamp() < rhs.ballot.timestamp();
}

inline bool operator>(const proposal& lhs, const proposal& rhs) {
    return lhs.ballot.timestamp() > rhs.ballot.timestamp();
}

// Used for logging and debugging.
std::ostream& operator<<(std::ostream& os, const proposal& proposal);

} // end of namespace "paxos"
} // end of namespace "service"
