/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "utils/error_injection.hh"

namespace utils {

logging::logger errinj_logger("debug_error_injection");

thread_local error_injection<false> error_injection<false>::_local;

} // namespace utils
