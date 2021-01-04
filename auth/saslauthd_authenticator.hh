/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "auth/authenticator.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
}

namespace auth {

/// Delegates authentication to saslauthd.  When this class is asked to authenticate, it passes the credentials
/// to saslauthd, gets its response, and allows or denies authentication based on that response.
class saslauthd_authenticator : public authenticator {
    sstring _socket_path; ///< Path to the domain socket on which saslauthd is listening.
public:
    saslauthd_authenticator(cql3::query_processor&, ::service::migration_manager&);

    future<> start() override;

    future<> stop() override;

    std::string_view qualified_java_name() const override;

    bool require_authentication() const override;

    authentication_option_set supported_options() const override;

    authentication_option_set alterable_options() const override;

    future<authenticated_user> authenticate(const credentials_map& credentials) const override;

    future<> create(std::string_view role_name, const authentication_options& options) const override;

    future<> alter(std::string_view role_name, const authentication_options& options) const override;

    future<> drop(std::string_view role_name) const override;

    future<custom_options> query_custom_options(std::string_view role_name) const override;

    const resource_set& protected_resources() const override;

    ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;
};

/// A set of four credential strings that saslauthd expects.
struct saslauthd_credentials {
    sstring username, password, service, realm;
};

future<bool> authenticate_with_saslauthd(sstring saslauthd_socket_path, const saslauthd_credentials& creds);

}

