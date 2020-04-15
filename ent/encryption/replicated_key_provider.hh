/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "encryption.hh"

namespace encryption {

class replicated_key_provider_factory : public key_provider_factory {
public:
    replicated_key_provider_factory();
    ~replicated_key_provider_factory();

    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;

    static void init();
};

}
