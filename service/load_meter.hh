/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma  once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include "service/load_broadcaster.hh"

using namespace seastar;

class database;
namespace gms { class gossiper; }

namespace service {

class load_meter {
private:
    shared_ptr<load_broadcaster> _lb;

    /** raw load value */
    double get_load() const;
    sstring get_load_string() const;

public:
    future<std::map<sstring, double>> get_load_map();

    future<> init(distributed<database>& db, gms::gossiper& gossiper);
    future<> exit();
};

}
