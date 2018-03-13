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
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/cf_prop_defs.hh"

namespace cql3 {

namespace statements {

/**
 * Class for common statement properties.
 */
class cf_properties final {
    const ::shared_ptr<cf_prop_defs> _properties = ::make_shared<cf_prop_defs>();
    bool _use_compact_storage = false;
    std::vector<std::pair<::shared_ptr<column_identifier>, bool>> _defined_ordering; // Insertion ordering is important
public:
    auto& properties() const {
        return _properties;
    }

    bool use_compact_storage() const {
        return _use_compact_storage;
    }

    void set_compact_storage() {
        _use_compact_storage = true;
    }

    auto& defined_ordering() const {
        return _defined_ordering;
    }

    data_type get_reversable_type(::shared_ptr<column_identifier> t, data_type type) const {
        auto is_reversed = find_ordering_info(t).value_or(false);
        if (!is_reversed && type->is_reversed()) {
            return static_pointer_cast<const reversed_type_impl>(type)->underlying_type();
        }
        if (is_reversed && !type->is_reversed()) {
            return reversed_type_impl::get_instance(type);
        }
        return type;
    }

    std::experimental::optional<bool> find_ordering_info(::shared_ptr<column_identifier> type) const {
        for (auto& t: _defined_ordering) {
            if (*(t.first) == *type) {
                return t.second;
            }
        }
        return {};
    }

    void set_ordering(::shared_ptr<column_identifier> alias, bool reversed) {
        _defined_ordering.emplace_back(alias, reversed);
    }

    void validate() {
        _properties->validate();
    }
};

}

}
