/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "auth/authentication_options.hh"

#include <iostream>

namespace auth {

std::ostream& operator<<(std::ostream& os, authentication_option a) {
    switch (a) {
        case authentication_option::password: os << "PASSWORD"; break;
        case authentication_option::options: os << "OPTIONS"; break;
    }

    return os;
}

}
