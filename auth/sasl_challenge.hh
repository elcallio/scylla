/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <functional>
#include <optional>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "auth/authenticated_user.hh"
#include "bytes.hh"
#include "seastarx.hh"

namespace auth {

///
/// A stateful SASL challenge which supports many authentication schemes (depending on the implementation).
///
class sasl_challenge {
public:
    virtual ~sasl_challenge() = default;

    virtual bytes evaluate_response(bytes_view client_response) = 0;

    virtual bool is_complete() const = 0;

    virtual future<authenticated_user> get_authenticated_user() const = 0;

    virtual const sstring& get_username() const = 0;
};

class plain_sasl_challenge : public sasl_challenge {
public:
    using completion_callback = std::function<future<authenticated_user>(std::string_view, std::string_view)>;

    explicit plain_sasl_challenge(completion_callback f) : _when_complete(std::move(f)) {
    }

    virtual bytes evaluate_response(bytes_view) override;

    virtual bool is_complete() const override;

    virtual future<authenticated_user> get_authenticated_user() const override;

    virtual const sstring& get_username() const override;

private:
    std::optional<sstring> _username, _password;
    completion_callback _when_complete;
};

}
