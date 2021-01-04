
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "database.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, value);
};

int main(int argc, char* argv[]) {
    return app_template().run_deprecated(argc, argv, [] {
        auto s = make_shared_schema({}, "ks", "cf",
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);

        memtable mt(s);

        std::cout << "Timing mutation of single column within one row...\n";

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
        bytes value = int32_type->decompose(3);

        time_it([&] {
            mutation m(s, key);
            const column_definition& col = *s->get_column_definition("r1");
            m.set_clustered_cell(c_key, col, make_atomic_cell(col.type, value));
            mt.apply(std::move(m));
        });
        engine().exit(0);
    });
}
