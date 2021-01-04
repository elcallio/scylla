/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "seastarx.hh"

class is_preemptible_tag;
using is_preemptible = bool_class<is_preemptible_tag>;
