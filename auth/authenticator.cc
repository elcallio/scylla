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
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "authenticator.hh"
#include "authenticated_user.hh"
#include "common.hh"
#include "password_authenticator.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "utils/class_registrator.hh"

const sstring auth::authenticator::USERNAME_KEY("username");
const sstring auth::authenticator::PASSWORD_KEY("password");

auth::authenticator::option auth::authenticator::string_to_option(const sstring& name) {
    if (strcasecmp(name.c_str(), "password") == 0) {
        return option::PASSWORD;
    }
    throw std::invalid_argument(name);
}

sstring auth::authenticator::option_to_string(option opt) {
    switch (opt) {
    case option::PASSWORD:
        return "PASSWORD";
    default:
        throw std::invalid_argument(sprint("Unknown option {}", opt));
    }
}
