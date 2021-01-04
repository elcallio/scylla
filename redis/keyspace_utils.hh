/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include  "db/config.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"

using namespace seastar;

namespace redis {

static constexpr auto DATA_COLUMN_NAME = "data";
static constexpr auto STRINGs         = "STRINGs";
static constexpr auto LISTs           = "LISTs";
static constexpr auto HASHes          = "HASHes";
static constexpr auto SETs            = "SETs";
static constexpr auto ZSETs           = "ZSETs";

future<> maybe_create_keyspace(db::config& cfg);

}
