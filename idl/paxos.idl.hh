/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace service {
namespace paxos {

class proposal {
    utils::UUID ballot;
    frozen_mutation update;
};

class promise {
    std::optional<service::paxos::proposal> accepted_proposal;
    std::optional<service::paxos::proposal> most_recent_commit;
    std::optional<std::variant<query::result, query::result_digest>> get_data_or_digest();
};

}
}
