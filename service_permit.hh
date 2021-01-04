/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

class service_permit {
    seastar::lw_shared_ptr<seastar::semaphore_units<>> _permit;
    service_permit(seastar::semaphore_units<>&& u) : _permit(seastar::make_lw_shared<seastar::semaphore_units<>>(std::move(u))) {}
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit);
    friend service_permit empty_service_permit();
};

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit) {
    return service_permit(std::move(permit));
}

inline service_permit empty_service_permit() {
    return make_service_permit(seastar::semaphore_units<>());
}
