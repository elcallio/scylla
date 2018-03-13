/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/column_identifier.hh"
#include "exceptions/exceptions.hh"
#include "cql3/selection/simple_selector.hh"

#include <regex>

namespace cql3 {

column_identifier::column_identifier(sstring raw_text, bool keep_case) {
    _text = std::move(raw_text);
    if (!keep_case) {
        std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
    }
    bytes_ = to_bytes(_text);
}

column_identifier::column_identifier(bytes bytes_, data_type type)
    : bytes_(std::move(bytes_))
    , _text(type->get_string(this->bytes_))
{ }

column_identifier::column_identifier(bytes bytes_, sstring text)
    : bytes_(std::move(bytes_))
    , _text(std::move(text))
{ }

bool column_identifier::operator==(const column_identifier& other) const {
    return bytes_ == other.bytes_;
}

const sstring& column_identifier::text() const {
    return _text;
}

const bytes& column_identifier::name() const {
    return bytes_;
}

sstring column_identifier::to_string() const {
    return _text;
}

sstring column_identifier::to_cql_string() const {
    static const std::regex unquoted_identifier_re("[a-z][a-z0-9_]*");
    if (std::regex_match(_text.begin(), _text.end(), unquoted_identifier_re)) {
        return _text;
    }
    static const std::regex double_quote_re("\"");
    std::string result = _text;
    std::regex_replace(result, double_quote_re, "\"\"");
    return '"' + result + '"';
}

column_identifier::raw::raw(sstring raw_text, bool keep_case)
    : _raw_text{raw_text}
    , _text{raw_text}
{
    if (!keep_case) {
        std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
    }
}

::shared_ptr<selection::selectable> column_identifier::raw::prepare(schema_ptr s) {
    return prepare_column_identifier(s);
}

::shared_ptr<column_identifier>
column_identifier::raw::prepare_column_identifier(schema_ptr schema) {
    if (schema->regular_column_name_type() == utf8_type) {
        return ::make_shared<column_identifier>(_text, true);
    }

    // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
    // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
    // make a special check for those and treat them normally.  See CASSANDRA-8178.
    auto text_bytes = to_bytes(_text);
    auto def = schema->get_column_definition(text_bytes);
    if (def) {
        return ::make_shared<column_identifier>(std::move(text_bytes), _text);
    }

    return ::make_shared<column_identifier>(schema->regular_column_name_type()->from_string(_raw_text), _text);
}

bool column_identifier::raw::processes_selection() const {
    return false;
}

bool column_identifier::raw::operator==(const raw& other) const {
    return _text == other._text;
}

bool column_identifier::raw::operator!=(const raw& other) const {
    return !operator==(other);
}

sstring column_identifier::raw::to_string() const {
    return _text;
}

std::ostream& operator<<(std::ostream& out, const column_identifier::raw& id) {
    return out << id._text;
}

::shared_ptr<selection::selector::factory>
column_identifier::new_selector_factory(database& db, schema_ptr schema, std::vector<const column_definition*>& defs) {
    auto def = get_column_definition(schema, *this);
    if (!def) {
        throw exceptions::invalid_request_exception(sprint("Undefined name %s in selection clause", _text));
    }

    return selection::simple_selector::new_factory(def->name_as_text(), add_and_get_index(*def, defs), def->type);
}

}

bool cql3::column_identifier::text_comparator::operator()(const cql3::column_identifier& c1, const cql3::column_identifier& c2) const {
    return c1.text() < c2.text();
}
