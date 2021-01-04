/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace netw {

struct schema_pull_options {
    bool remote_supports_canonical_mutation_retval;
};

} // namespace netw
