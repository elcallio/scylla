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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "exceptions.hh"
#include <seastar/core/shared_ptr.hh>
#include "cql3/column_identifier.hh"
#include "cql3/relation.hh"

namespace exceptions {

/**
 * Exception thrown when an entity is not recognized within a relation.
 */
class unrecognized_entity_exception : public invalid_request_exception {
public:
    /**
     * The unrecognized entity.
     */
    cql3::column_identifier entity;

    /**
     * The entity relation in a stringified form.
     */
    sstring relation_str;

    /**
     * Creates a new <code>UnrecognizedEntityException</code>.
     * @param entity the unrecognized entity
     * @param relation_str the entity relation string
     */
    unrecognized_entity_exception(cql3::column_identifier entity, sstring relation_str)
        : invalid_request_exception(format("Undefined name {} in where clause ('{}')", entity, relation_str))
        , entity(std::move(entity))
        , relation_str(std::move(relation_str))
    { }
};

}
