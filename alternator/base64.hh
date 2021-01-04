/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string_view>
#include "bytes.hh"
#include "utils/rjson.hh"

std::string base64_encode(bytes_view);

bytes base64_decode(std::string_view);

inline bytes base64_decode(const rjson::value& v) {
  return base64_decode(std::string_view(v.GetString(), v.GetStringLength()));
}

size_t base64_decoded_len(std::string_view str);

bool base64_begins_with(std::string_view base, std::string_view operand);
