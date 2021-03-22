/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <iterator>
#include "bytes.hh"
#include "cql_serialization_format.hh"

int read_collection_size(bytes_view& in, cql_serialization_format sf);
bytes_view read_collection_value(bytes_view& in, cql_serialization_format sf);

template <FragmentedView View>
int read_collection_size(View& in, cql_serialization_format);

template <FragmentedView View>
View read_collection_value(View& in, cql_serialization_format);

// iterator that takes a set or list in serialized form, and emits
// each element, still in serialized form
class listlike_partial_deserializing_iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = bytes_view;
    using difference_type = std::ptrdiff_t;
    using pointer = bytes_view*;
    using reference = bytes_view&;
private:
    bytes_view* _in;
    int _remain;
    bytes_view _cur;
    cql_serialization_format _sf;
private:
    struct end_tag {};
    listlike_partial_deserializing_iterator(bytes_view& in, cql_serialization_format sf)
            : _in(&in), _sf(sf) {
        _remain = read_collection_size(*_in, _sf);
        parse();
    }
    listlike_partial_deserializing_iterator(end_tag)
            : _remain(0), _sf(cql_serialization_format::internal()) {  // _sf is bogus, but doesn't matter
    }
public:
    bytes_view operator*() const { return _cur; }
    listlike_partial_deserializing_iterator& operator++() {
        --_remain;
        parse();
        return *this;
    }
    void operator++(int) {
        --_remain;
        parse();
    }
    bool operator==(const listlike_partial_deserializing_iterator& x) const {
        return _remain == x._remain;
    }
    bool operator!=(const listlike_partial_deserializing_iterator& x) const {
        return _remain != x._remain;
    }
    static listlike_partial_deserializing_iterator begin(bytes_view& in, cql_serialization_format sf) {
        return { in, sf };
    }
    static listlike_partial_deserializing_iterator end(bytes_view in, cql_serialization_format sf) {
        return { end_tag() };
    }
private:
    void parse() {
        if (_remain) {
            _cur = read_collection_value(*_in, _sf);
        } else {
            _cur = {};
        }
    }
};
