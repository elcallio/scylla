/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <map>
#include "exceptions/exceptions.hh"



namespace qos {

/**
 *  a structure that holds the configuration for
 *  a service level.
 */
struct service_level_options {
    int shares;
};

/**
 * The service level options comparison operators helps to determine if
 * a change was introduced to the service level.
 */
bool operator==(const service_level_options& lhs, const service_level_options& rhs);
bool operator!=(const service_level_options& lhs, const service_level_options& rhs);

using service_levels_info = std::map<sstring, service_level_options>;

///
/// A logical argument error for a service_level statement operation.
///
class service_level_argument_exception : public exceptions::invalid_request_exception {
public:
    using exceptions::invalid_request_exception::invalid_request_exception;
};

///
/// An exception to indicate that the service level given as parameter doesn't exist.
///
class nonexistant_service_level_exception : public service_level_argument_exception {
public:
    nonexistant_service_level_exception(sstring service_level_name)
            : service_level_argument_exception(format("Service Level {} doesn't exists.", service_level_name)) {
    }
};

}
