/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "encryption.hh"
#include "system_key.hh"

namespace encryption {

class kmip_key_provider_factory : public key_provider_factory {
public:
    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;
};

class kmip_host;

class kmip_system_key : public system_key {
    shared_ptr<symmetric_key> _key;
    shared_ptr<kmip_host> _host;
    bytes _id;
    sstring _name;
public:
    kmip_system_key(encryption_context&, const sstring&);
    ~kmip_system_key();

    static bool is_kmip_path(const sstring&);

    future<shared_ptr<symmetric_key>> get_key() override;
    const sstring& name() const override;
    bool is_local() const override {
        return false;
    }
};

}