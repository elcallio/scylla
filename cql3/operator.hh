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

#include <cstddef>
#include <iosfwd>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace cql3 {

class operator_type {
public:
    static const operator_type EQ;
    static const operator_type LT;
    static const operator_type LTE;
    static const operator_type GTE;
    static const operator_type GT;
    static const operator_type IN;
    static const operator_type CONTAINS;
    static const operator_type CONTAINS_KEY;
    static const operator_type NEQ;
    static const operator_type IS_NOT;
    static const operator_type LIKE;
private:
    int32_t _b;
    const operator_type& _reverse;
    sstring _text;
private:
    operator_type(int32_t b, const operator_type& reverse, sstring text)
        : _b(b)
        , _reverse(reverse)
        , _text(std::move(text))
    {}
public:
    operator_type(const operator_type&) = delete;
    operator_type& operator=(const operator_type&) = delete;
    const operator_type& reverse() const { return _reverse; }
    bool is_slice() const {
        return (*this == LT) || (*this == LTE) || (*this == GT) || (*this == GTE);
    }
    bool is_compare() const {
        // EQ, LT, LTE, GT, GTE, NEQ
        return _b < 5 || _b == 8;
    }
    sstring to_string() const { return _text; }
    bool operator==(const operator_type& other) const { return this == &other; }
    bool operator!=(const operator_type& other) const { return this != &other; }
#if 0

    /**
     * Write the serialized version of this <code>Operator</code> to the specified output.
     *
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs while writing to the specified output
     */
    public void writeTo(DataOutput output) throws IOException
    {
        output.writeInt(b);
    }

    /**
     * Deserializes a <code>Operator</code> instance from the specified input.
     *
     * @param input the input to read from
     * @return the <code>Operator</code> instance deserialized
     * @throws IOException if a problem occurs while deserializing the <code>Type</code> instance.
     */
    public static Operator readFrom(DataInput input) throws IOException
    {
          int b = input.readInt();
          for (Operator operator : values())
              if (operator.b == b)
                  return operator;

          throw new IOException(String.format("Cannot resolve Relation.Type from binary representation: %s", b));
    }
#endif
};

static inline
std::ostream& operator<<(std::ostream& out, const operator_type& op) {
    return out << op.to_string();
}

}
