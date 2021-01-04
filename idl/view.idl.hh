/*
 * Copyright 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace db {
namespace view {
class update_backlog {
    size_t current;
    size_t max;
};
}
}
