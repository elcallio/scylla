/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include <memory>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "db/timeout_clock.hh"
#include "exceptions/exceptions.hh"
#include "timeout_config.hh"

class database;

namespace service {
class storage_proxy;
}


namespace db {
struct query_context {
    distributed<database>& _db;
    distributed<cql3::query_processor>& _qp;
    query_context(distributed<database>& db, distributed<cql3::query_processor>& qp) : _db(db), _qp(qp) {}

    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring req, Args&&... args) {
        return _qp.local().execute_internal(req, { data_value(std::forward<Args>(args))... });
    }

    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql_with_timeout(sstring req,
            db::timeout_clock::time_point timeout,
            Args&&... args) {
        const db::timeout_clock::time_point now = db::timeout_clock::now();
        const db::timeout_clock::duration d =
            now < timeout ?
                timeout - now :
                // let the `storage_proxy` time out the query down the call chain
                db::timeout_clock::duration::zero();

        return do_with(timeout_config{d, d, d, d, d, d, d}, [this, req = std::move(req), &args...] (auto& tcfg) {
            return _qp.local().process(req,
                cql3::query_options::DEFAULT.get_consistency(),
                tcfg,
                { data_value(std::forward<Args>(args))... },
                true);
        });
    }

    database& db() {
        return _db.local();
    }

    service::storage_proxy& proxy() {
        return _qp.local().proxy();
    }

    cql3::query_processor& qp() {
        return _qp.local();
    }
};

// This does not have to be thread local, because all cores will share the same context.
extern std::unique_ptr<query_context> qctx;

template <typename... Args>
static future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring text, Args&&... args) {
    assert(qctx);
    return qctx->execute_cql(text, std::forward<Args>(args)...);
}

template <typename... Args>
static future<::shared_ptr<cql3::untyped_result_set>> execute_cql_with_timeout(sstring cql,
        db::timeout_clock::time_point timeout,
        Args&&... args) {
    assert(qctx);
    return qctx->execute_cql_with_timeout(cql, timeout, std::forward<Args>(args)...);
}

}
