/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "types.hh"
#include "utils/rjson.hh"

bytes from_json_object(const abstract_type &t, const rjson::value& value, cql_serialization_format sf);
sstring to_json_string(const abstract_type &t, bytes_view bv);
inline sstring to_json_string(const abstract_type &t, const bytes& b) {
    return to_json_string(t, bytes_view(b));
}

inline sstring to_json_string(const abstract_type& t, const bytes_opt& b) {
    return b ? to_json_string(t, *b) : "null";
}
