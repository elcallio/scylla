/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include "multiprecision_int.hh"
#include <iostream>

namespace utils {

std::string multiprecision_int::str() const {
    return _v.str();
}

std::ostream& operator<<(std::ostream& os, const multiprecision_int& x) {
    return os << x._v;
}

}

