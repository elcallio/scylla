
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/net/socket_defs.hh>

// Common values used in multiple LDAP tests.
namespace {

constexpr auto base_dn = "dc=example,dc=com";
constexpr auto manager_dn = "cn=root,dc=example,dc=com";
constexpr auto manager_password = "secret";
const auto ldap_envport = std::getenv("SEASTAR_LDAP_PORT");
const std::string ldap_port(ldap_envport ? ldap_envport : "389");
const socket_address local_ldap_address(ipv4_addr("127.0.0.1", std::stoi(ldap_port)));
const socket_address local_fail_inject_address(ipv4_addr("127.0.0.1", std::stoi(ldap_port) + 2));

} // anonymous namespace
