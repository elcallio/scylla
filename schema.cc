/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "utils/UUID_gen.hh"
#include "cql3/column_identifier.hh"
#include "cql3/util.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "db/marshal/type_parser.hh"
#include "version.hh"
#include "schema_registry.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "view_info.hh"
#include "partition_slice_builder.hh"

constexpr int32_t schema::NAME_LENGTH;

void column_mask::union_with(const column_mask& with) {

    // boost::dynamic_bitset doesn't support logical
    // or of bitsets of different sizes, work this around.
    if (_mask.size() > with._mask.size()) {
        if (with._mask.size()) {
            column_mask::bitset tmp = with._mask;
            tmp.resize(_mask.size());
            _mask |= tmp;
        }
        return;
    }
    if (_mask.size() < with._mask.size()) {
        _mask.resize(with._mask.size());
    }
    _mask |= with._mask;
}

sstring to_sstring(column_kind k) {
    switch (k) {
    case column_kind::partition_key:  return "PARTITION_KEY";
    case column_kind::clustering_key: return "CLUSTERING_COLUMN";
    case column_kind::static_column:  return "STATIC";
    case column_kind::regular_column: return "REGULAR";
    }
    throw std::invalid_argument("unknown column kind");
}

bool is_compatible(column_kind k1, column_kind k2) {
    return k1 == k2;
}

column_mapping_entry::column_mapping_entry(bytes name, sstring type_name)
    : column_mapping_entry(std::move(name), db::marshal::type_parser::parse(type_name))
{
}

column_mapping_entry::column_mapping_entry(const column_mapping_entry& o)
    : column_mapping_entry(o._name, o._type->name())
{
}

column_mapping_entry& column_mapping_entry::operator=(const column_mapping_entry& o) {
    auto copy = o;
    return operator=(std::move(copy));
}

template<typename Sequence>
std::vector<data_type>
get_column_types(const Sequence& column_definitions) {
    std::vector<data_type> result;
    for (auto&& col : column_definitions) {
        result.push_back(col.type);
    }
    return result;
}

std::ostream& operator<<(std::ostream& out, const column_mapping& cm) {
    column_id n_static = cm.n_static();
    column_id n_regular = cm.columns().size() - n_static;

    auto pr_entry = [] (column_id i, const column_mapping_entry& e) {
        // Without schema we don't know if name is UTF8. If we had schema we could use
        // s->regular_column_name_type()->to_string(e.name()).
        return format("{{id={}, name=0x{}, type={}}}", i, e.name(), e.type()->name());
    };

    return out << "{static=[" << ::join(", ", boost::irange<column_id>(0, n_static) |
            boost::adaptors::transformed([&] (column_id i) { return pr_entry(i, cm.static_column_at(i)); }))
        << "], regular=[" << ::join(", ", boost::irange<column_id>(0, n_regular) |
            boost::adaptors::transformed([&] (column_id i) { return pr_entry(i, cm.regular_column_at(i)); }))
        << "]}";
}

std::ostream& operator<<(std::ostream& os, ordinal_column_id id)
{
    return os << static_cast<column_count_type>(id);
}

::shared_ptr<cql3::column_specification>
schema::make_column_specification(const column_definition& def) {
    auto id = ::make_shared<cql3::column_identifier>(def.name(), column_name_type(def));
    return ::make_shared<cql3::column_specification>(_raw._ks_name, _raw._cf_name, std::move(id), def.type);
}

v3_columns::v3_columns(std::vector<column_definition> cols, bool is_dense, bool is_compound)
    : _is_dense(is_dense)
    , _is_compound(is_compound)
    , _columns(std::move(cols))
{
    for (column_definition& def : _columns) {
        _columns_by_name[def.name()] = &def;
    }
}

v3_columns v3_columns::from_v2_schema(const schema& s) {
    data_type static_column_name_type = utf8_type;
    std::vector<column_definition> cols;

    if (s.is_static_compact_table()) {
        if (s.has_static_columns()) {
            throw std::runtime_error(
                format("v2 static compact table should not have static columns: {}.{}", s.ks_name(), s.cf_name()));
        }
        if (s.clustering_key_size()) {
            throw std::runtime_error(
                format("v2 static compact table should not have clustering columns: {}.{}", s.ks_name(), s.cf_name()));
        }
        static_column_name_type = s.regular_column_name_type();
        for (auto& c : s.all_columns()) {
            // Note that for "static" no-clustering compact storage we use static for the defined columns
            if (c.kind == column_kind::regular_column) {
                auto new_def = c;
                new_def.kind = column_kind::static_column;
                cols.push_back(new_def);
            } else {
                cols.push_back(c);
            }
        }
        schema_builder::default_names names(s._raw);
        cols.emplace_back(to_bytes(names.clustering_name()), static_column_name_type, column_kind::clustering_key, 0);
        cols.emplace_back(to_bytes(names.compact_value_name()), s.make_legacy_default_validator(), column_kind::regular_column, 0);
    } else {
        cols = s.all_columns();
    }

    for (column_definition& def : cols) {
        data_type name_type = def.is_static() ? static_column_name_type : utf8_type;
        auto id = ::make_shared<cql3::column_identifier>(def.name(), name_type);
        def.column_specification = ::make_shared<cql3::column_specification>(s.ks_name(), s.cf_name(), std::move(id), def.type);
    }

    return v3_columns(std::move(cols), s.is_dense(), s.is_compound());
}

void v3_columns::apply_to(schema_builder& builder) const {
    if (is_static_compact()) {
        for (auto& c : _columns) {
            if (c.kind == column_kind::regular_column) {
                builder.set_default_validation_class(c.type);
            } else if (c.kind == column_kind::static_column) {
                auto new_def = c;
                new_def.kind = column_kind::regular_column;
                builder.with_column(new_def);
            } else if (c.kind == column_kind::clustering_key) {
                builder.set_regular_column_name_type(c.type);
            } else {
                builder.with_column(c);
            }
        }
    } else {
        for (auto& c : _columns) {
            if (is_compact() && c.kind == column_kind::regular_column) {
                builder.set_default_validation_class(c.type);
            }
            builder.with_column(c);
        }
    }
}

bool v3_columns::is_static_compact() const {
    return !_is_dense && !_is_compound;
}

bool v3_columns::is_compact() const {
    return _is_dense || !_is_compound;
}

const std::unordered_map<bytes, const column_definition*>& v3_columns::columns_by_name() const {
    return _columns_by_name;
}

const std::vector<column_definition>& v3_columns::all_columns() const {
    return _columns;
}

void schema::rebuild() {
    _partition_key_type = make_lw_shared<compound_type<>>(get_column_types(partition_key_columns()));
    _clustering_key_type = make_lw_shared<compound_prefix>(get_column_types(clustering_key_columns()));
    _clustering_key_size = column_offset(column_kind::static_column) - column_offset(column_kind::clustering_key);
    _regular_column_count = _raw._columns.size() - column_offset(column_kind::regular_column);
    _static_column_count = column_offset(column_kind::regular_column) - column_offset(column_kind::static_column);
    _columns_by_name.clear();

    for (const column_definition& def : all_columns()) {
        _columns_by_name[def.name()] = &def;
    }

    static_assert(row_column_ids_are_ordered_by_name::value, "row columns don't need to be ordered by name");
    if (!std::is_sorted(regular_columns().begin(), regular_columns().end(), column_definition::name_comparator(regular_column_name_type()))) {
        throw std::runtime_error("Regular columns should be sorted by name");
    }
    if (!std::is_sorted(static_columns().begin(), static_columns().end(), column_definition::name_comparator(static_column_name_type()))) {
        throw std::runtime_error("Static columns should be sorted by name");
    }

    {
        std::vector<column_mapping_entry> cm_columns;
        for (const column_definition& def : boost::range::join(static_columns(), regular_columns())) {
            cm_columns.emplace_back(column_mapping_entry{def.name(), def.type});
        }
        _column_mapping = column_mapping(std::move(cm_columns), static_columns_count());
    }

    thrift()._compound = is_compound();
    thrift()._is_dynamic = clustering_key_size() > 0;

    if (is_counter()) {
        for (auto&& cdef : boost::range::join(static_columns(), regular_columns())) {
            if (!cdef.type->is_counter()) {
                throw exceptions::configuration_exception(format("Cannot add a non counter column ({}) in a counter column family", cdef.name_as_text()));
            }
        }
    } else {
        for (auto&& cdef : all_columns()) {
            if (cdef.type->is_counter()) {
                throw exceptions::configuration_exception(format("Cannot add a counter column ({}) in a non counter column family", cdef.name_as_text()));
            }
        }
    }

    _v3_columns = v3_columns::from_v2_schema(*this);
    _full_slice = make_shared(partition_slice_builder(*this).build());
}

const column_mapping& schema::get_column_mapping() const {
    return _column_mapping;
}

schema::raw_schema::raw_schema(utils::UUID id)
    : _id(id)
{ }

schema::schema(const raw_schema& raw, std::optional<raw_view_info> raw_view_info)
    : _raw(raw)
    , _offsets([this] {
        if (_raw._columns.size() > std::numeric_limits<column_count_type>::max()) {
            throw std::runtime_error(format("Column count limit ({:d}) overflowed: {:d}",
                                            std::numeric_limits<column_count_type>::max(), _raw._columns.size()));
        }

        auto& cols = _raw._columns;
        std::array<column_count_type, 4> count = { 0, 0, 0, 0 };
        auto i = cols.begin();
        auto e = cols.end();
        for (auto k : { column_kind::partition_key, column_kind::clustering_key, column_kind::static_column, column_kind::regular_column }) {
            auto j = std::stable_partition(i, e, [k](const auto& c) {
                return c.kind == k;
            });
            count[column_count_type(k)] = std::distance(i, j);
            i = j;
        }
        return std::array<column_count_type, 3> {
                count[0],
                count[0] + count[1],
                count[0] + count[1] + count[2],
        };
    }())
{
    std::sort(
            _raw._columns.begin() + column_offset(column_kind::static_column),
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            column_definition::name_comparator(static_column_name_type()));
    std::sort(
            _raw._columns.begin()
                    + column_offset(column_kind::regular_column),
            _raw._columns.end(), column_definition::name_comparator(regular_column_name_type()));

    std::sort(_raw._columns.begin(),
              _raw._columns.begin() + column_offset(column_kind::clustering_key),
              [] (auto x, auto y) { return x.id < y.id; });
    std::sort(_raw._columns.begin() + column_offset(column_kind::clustering_key),
              _raw._columns.begin() + column_offset(column_kind::static_column),
              [] (auto x, auto y) { return x.id < y.id; });

    column_id id = 0;
    for (auto& def : _raw._columns) {
        def.column_specification = make_column_specification(def);
        assert(!def.id || def.id == id - column_offset(def.kind));
        def.ordinal_id = static_cast<ordinal_column_id>(id);
        def.id = id - column_offset(def.kind);

        auto dropped_at_it = _raw._dropped_columns.find(def.name_as_text());
        if (dropped_at_it != _raw._dropped_columns.end()) {
            def._dropped_at = std::max(def._dropped_at, dropped_at_it->second.timestamp);
        }

        def._thrift_bits = column_definition::thrift_bits();

        {
            // is_on_all_components
            // TODO : In origin, this predicate is "componentIndex == null", which is true in
            // a number of cases, some of which I've most likely missed...
            switch (def.kind) {
            case column_kind::partition_key:
                // In origin, ci == null is true for a PK column where CFMetaData "keyValidator" is non-composite.
                // Which is true of #pk == 1
                def._thrift_bits.is_on_all_components = partition_key_size() == 1;
                break;
            case column_kind::regular_column:
                if (_raw._is_dense) {
                    // regular values in dense tables are alone, so they have no index
                    def._thrift_bits.is_on_all_components = true;
                    break;
                }
            default:
                // Or any other column where "comparator" is not compound
                def._thrift_bits.is_on_all_components = !thrift().has_compound_comparator();
                break;
            }
        }

        ++id;
    }

    rebuild();
    if (raw_view_info) {
        _view_info = std::make_unique<::view_info>(*this, *raw_view_info);
    }
}

schema::schema(std::optional<utils::UUID> id,
    sstring ks_name,
    sstring cf_name,
    std::vector<column> partition_key,
    std::vector<column> clustering_key,
    std::vector<column> regular_columns,
    std::vector<column> static_columns,
    data_type regular_column_name_type,
    sstring comment)
    : schema([&] {
        raw_schema raw(id ? *id : utils::UUID_gen::get_time_UUID());

        raw._comment = std::move(comment);
        raw._ks_name = std::move(ks_name);
        raw._cf_name = std::move(cf_name);
        raw._regular_column_name_type = regular_column_name_type;

        auto build_columns = [&raw](std::vector<column>& columns, column_kind kind) {
            for (auto& sc : columns) {
                if (sc.type->is_multi_cell()) {
                    raw._collections.emplace(sc.name, sc.type);
                }
                raw._columns.emplace_back(std::move(sc.name), std::move(sc.type), kind);
            }
        };

        build_columns(partition_key, column_kind::partition_key);
        build_columns(clustering_key, column_kind::clustering_key);
        build_columns(static_columns, column_kind::static_column);
        build_columns(regular_columns, column_kind::regular_column);

        return raw;
    }(), std::nullopt)
{}

schema::schema(const schema& o)
    : _raw(o._raw)
    , _offsets(o._offsets)
{
    rebuild();
    if (o.is_view()) {
        _view_info = std::make_unique<::view_info>(*this, o.view_info()->raw());
    }
}

schema::~schema() {
    if (_registry_entry) {
        _registry_entry->detach_schema();
    }
}

schema_registry_entry*
schema::registry_entry() const noexcept {
    return _registry_entry;
}

sstring schema::thrift_key_validator() const {
    if (partition_key_size() == 1) {
        return partition_key_columns().begin()->type->name();
    } else {
        sstring type_params = ::join(", ", partition_key_columns()
                            | boost::adaptors::transformed(std::mem_fn(&column_definition::type))
                            | boost::adaptors::transformed(std::mem_fn(&abstract_type::name)));
        return "org.apache.cassandra.db.marshal.CompositeType(" + type_params + ")";
    }
}

bool
schema::has_multi_cell_collections() const {
    return boost::algorithm::any_of(all_columns(), [] (const column_definition& cdef) {
        return cdef.type->is_collection() && cdef.type->is_multi_cell();
    });
}

bool operator==(const schema& x, const schema& y)
{
    return x._raw._id == y._raw._id
        && x._raw._ks_name == y._raw._ks_name
        && x._raw._cf_name == y._raw._cf_name
        && x._raw._columns == y._raw._columns
        && x._raw._comment == y._raw._comment
        && x._raw._default_time_to_live == y._raw._default_time_to_live
        && x._raw._regular_column_name_type == y._raw._regular_column_name_type
        && x._raw._bloom_filter_fp_chance == y._raw._bloom_filter_fp_chance
        && x._raw._compressor_params == y._raw._compressor_params
        && x._raw._is_dense == y._raw._is_dense
        && x._raw._is_compound == y._raw._is_compound
        && x._raw._type == y._raw._type
        && x._raw._gc_grace_seconds == y._raw._gc_grace_seconds
        && x._raw._dc_local_read_repair_chance == y._raw._dc_local_read_repair_chance
        && x._raw._read_repair_chance == y._raw._read_repair_chance
        && x._raw._min_compaction_threshold == y._raw._min_compaction_threshold
        && x._raw._max_compaction_threshold == y._raw._max_compaction_threshold
        && x._raw._min_index_interval == y._raw._min_index_interval
        && x._raw._max_index_interval == y._raw._max_index_interval
        && x._raw._memtable_flush_period == y._raw._memtable_flush_period
        && x._raw._speculative_retry == y._raw._speculative_retry
        && x._raw._compaction_strategy == y._raw._compaction_strategy
        && x._raw._compaction_strategy_options == y._raw._compaction_strategy_options
        && x._raw._compaction_enabled == y._raw._compaction_enabled
        && x._raw._cdc_options == y._raw._cdc_options
        && x._raw._caching_options == y._raw._caching_options
        && x._raw._dropped_columns == y._raw._dropped_columns
        && x._raw._collections == y._raw._collections
        && indirect_equal_to<std::unique_ptr<::view_info>>()(x._view_info, y._view_info)
        && x._raw._indices_by_name == y._raw._indices_by_name
        && x._raw._is_counter == y._raw._is_counter
        && x._raw._in_memory == y._raw._in_memory
        ;
#if 0
        && Objects.equal(triggers, other.triggers)
#endif
}

index_metadata::index_metadata(const sstring& name,
                               const index_options_map& options,
                               index_metadata_kind kind,
                               is_local_index local)
    : _id{utils::UUID_gen::get_name_UUID(name)}
    , _name{name}
    , _kind{kind}
    , _options{options}
    , _local{bool(local)}
{}

bool index_metadata::operator==(const index_metadata& other) const {
    return _id == other._id
           && _name == other._name
           && _kind == other._kind
           && _options == other._options;
}

bool index_metadata::equals_noname(const index_metadata& other) const {
    return _kind == other._kind && _options == other._options;
}

const utils::UUID& index_metadata::id() const {
    return _id;
}

const sstring& index_metadata::name() const {
    return _name;
}

const index_metadata_kind index_metadata::kind() const {
    return _kind;
}

const index_options_map& index_metadata::options() const {
    return _options;
}

bool index_metadata::local() const {
    return _local;
}

sstring index_metadata::get_default_index_name(const sstring& cf_name,
                                               std::optional<sstring> root) {
    if (root) {
        return cf_name + "_" + root.value() + "_idx";
    }
    return cf_name + "_idx";
}

column_definition::column_definition(bytes name, data_type type, column_kind kind, column_id component_index, column_view_virtual is_view_virtual, column_computation_ptr computation, api::timestamp_type dropped_at)
        : _name(std::move(name))
        , _dropped_at(dropped_at)
        , _is_atomic(type->is_atomic())
        , _is_counter(type->is_counter())
        , _is_view_virtual(is_view_virtual)
        , _computation(std::move(computation))
        , type(std::move(type))
        , id(component_index)
        , kind(kind)
{}

std::ostream& operator<<(std::ostream& os, const column_definition& cd) {
    os << "ColumnDefinition{";
    os << "name=" << cd.name_as_text();
    os << ", type=" << cd.type->name();
    os << ", kind=" << to_sstring(cd.kind);
    if (cd.is_view_virtual()) {
        os << ", view_virtual";
    }
    if (cd.is_computed()) {
        os << ", computed:" << cd.get_computation().serialize();
    }
    os << ", componentIndex=" << (cd.has_component_index() ? std::to_string(cd.component_index()) : "null");
    os << ", droppedAt=" << cd._dropped_at;
    os << "}";
    return os;
}

const column_definition*
schema::get_column_definition(const bytes& name) const {
    auto i = _columns_by_name.find(name);
    if (i == _columns_by_name.end()) {
        return nullptr;
    }
    return i->second;
}

const column_definition&
schema::column_at(column_kind kind, column_id id) const {
    return _raw._columns.at(column_offset(kind) + id);
}

const column_definition&
schema::column_at(ordinal_column_id ordinal_id) const {
    return _raw._columns.at(static_cast<column_count_type>(ordinal_id));
}

std::ostream& operator<<(std::ostream& os, const schema& s) {
    os << "org.apache.cassandra.config.CFMetaData@" << &s << "[";
    os << "cfId=" << s._raw._id;
    os << ",ksName=" << s._raw._ks_name;
    os << ",cfName=" << s._raw._cf_name;
    os << ",cfType=" << cf_type_to_sstring(s._raw._type);
    os << ",comparator=" << cell_comparator::to_sstring(s);
    os << ",comment=" << s._raw._comment;
    os << ",readRepairChance=" << s._raw._read_repair_chance;
    os << ",dcLocalReadRepairChance=" << s._raw._dc_local_read_repair_chance;
    os << ",gcGraceSeconds=" << s._raw._gc_grace_seconds;
    os << ",keyValidator=" << s.thrift_key_validator();
    os << ",minCompactionThreshold=" << s._raw._min_compaction_threshold;
    os << ",maxCompactionThreshold=" << s._raw._max_compaction_threshold;
    os << ",columnMetadata=[";
    int n = 0;
    for (auto& cdef : s._raw._columns) {
        if (n++ != 0) {
            os << ", ";
        }
        os << cdef;
    }
    os << "]";
    os << ",compactionStrategyClass=class org.apache.cassandra.db.compaction." << sstables::compaction_strategy::name(s._raw._compaction_strategy);
    os << ",compactionStrategyOptions={";
    n = 0;
    for (auto& p : s._raw._compaction_strategy_options) {
        os << p.first << "=" << p.second;
        os << ", ";
    }
    os << "enabled=" << std::boolalpha << s._raw._compaction_enabled;
    os << "}";
    os << ",compressionParameters={";
    n = 0;
    for (auto& p : s._raw._compressor_params.get_options() ) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ",bloomFilterFpChance=" << s._raw._bloom_filter_fp_chance;
    os << ",memtableFlushPeriod=" << s._raw._memtable_flush_period;
    os << ",caching=" << s._raw._caching_options.to_sstring();
    os << ",cdc=" << s._raw._cdc_options.to_sstring();
    os << ",defaultTimeToLive=" << s._raw._default_time_to_live.count();
    os << ",minIndexInterval=" << s._raw._min_index_interval;
    os << ",maxIndexInterval=" << s._raw._max_index_interval;
    os << ",speculativeRetry=" << s._raw._speculative_retry.to_sstring();
    os << ",triggers=[]";
    os << ",isDense=" << std::boolalpha << s._raw._is_dense;
    os << ",in_memory=" << std::boolalpha << s._raw._in_memory;
    os << ",version=" << s.version();
    os << ",droppedColumns={";
    n = 0;
    for (auto& dc : s._raw._dropped_columns) {
        if (n++ != 0) {
            os << ", ";
        }
        os << dc.first << " : { " << dc.second.type->name() << ", " << dc.second.timestamp << " }";
    }
    os << "}";
    os << ",collections={";
    n = 0;
    for (auto& c : s._raw._collections) {
        if (n++ != 0) {
            os << ", ";
        }
        os << c.first << " : " << c.second->name();
    }
    os << "}";
    os << ",indices={";
    n = 0;
    for (auto& c : s._raw._indices_by_name) {
        if (n++ != 0) {
            os << ", ";
        }
        os << c.first << " : " << c.second.id();
    }
    os << "}";
    if (s.is_view()) {
        os << ", viewInfo=" << *s.view_info();
    }
    os << "]";
    return os;
}

const sstring&
column_definition::name_as_text() const {
    return column_specification->name->text();
}

const bytes&
column_definition::name() const {
    return _name;
}

sstring column_definition::name_as_cql_string() const {
    return cql3::util::maybe_quote(name_as_text());
}

bool column_definition::is_on_all_components() const {
    return _thrift_bits.is_on_all_components;
}

bool operator==(const column_definition& x, const column_definition& y)
{
    return x._name == y._name
        && x.type == y.type
        && x.id == y.id
        && x.kind == y.kind
        && x._dropped_at == y._dropped_at;
}

// Based on org.apache.cassandra.config.CFMetaData#generateLegacyCfId
utils::UUID
generate_legacy_id(const sstring& ks_name, const sstring& cf_name) {
    return utils::UUID_gen::get_name_UUID(ks_name + cf_name);
}

bool thrift_schema::has_compound_comparator() const {
    return _compound;
}

bool thrift_schema::is_dynamic() const {
    return _is_dynamic;
}

schema_builder::schema_builder(const sstring& ks_name, const sstring& cf_name,
        std::optional<utils::UUID> id, data_type rct)
        : _raw(id ? *id : utils::UUID_gen::get_time_UUID())
{
    _raw._ks_name = ks_name;
    _raw._cf_name = cf_name;
    _raw._regular_column_name_type = rct;
}

schema_builder::schema_builder(const schema_ptr s)
    : schema_builder(s->_raw)
{
    if (s->is_view()) {
        _view_info = s->view_info()->raw();
    }
}

schema_builder::schema_builder(const schema::raw_schema& raw)
    : _raw(raw)
{
    static_assert(schema::row_column_ids_are_ordered_by_name::value, "row columns don't need to be ordered by name");
    // Schema builder may add or remove columns and their ids need to be
    // recomputed in build().
    for (auto& def : _raw._columns | boost::adaptors::filtered([] (auto& def) { return !def.is_primary_key(); })) {
            def.id = 0;
            def.ordinal_id = static_cast<ordinal_column_id>(0);
    }
}

column_definition& schema_builder::find_column(const cql3::column_identifier& c) {
    auto i = std::find_if(_raw._columns.begin(), _raw._columns.end(), [c](auto& p) {
        return p.name() == c.name();
     });
    if (i != _raw._columns.end()) {
        return *i;
    }
    throw std::invalid_argument(format("No such column {}", c.name()));
}

schema_builder& schema_builder::with_column(const column_definition& c) {
    return with_column(bytes(c.name()), data_type(c.type), column_kind(c.kind), c.position(), c.view_virtual(), c.get_computation_ptr());
}

schema_builder& schema_builder::with_column(bytes name, data_type type, column_kind kind, column_view_virtual is_view_virtual) {
    // component_index will be determined by schema cosntructor
    return with_column(name, type, kind, 0, is_view_virtual);
}

schema_builder& schema_builder::with_column(bytes name, data_type type, column_kind kind, column_id component_index, column_view_virtual is_view_virtual, column_computation_ptr computation) {
    _raw._columns.emplace_back(name, type, kind, component_index, is_view_virtual, std::move(computation));
    if (type->is_multi_cell()) {
        with_collection(name, type);
    } else if (type->is_counter()) {
	    set_is_counter(true);
	}
    return *this;
}

schema_builder& schema_builder::with_computed_column(bytes name, data_type type, column_kind kind, column_computation_ptr computation) {
    return with_column(name, type, kind, 0, column_view_virtual::no, std::move(computation));
}

schema_builder& schema_builder::remove_column(bytes name)
{
    auto it = boost::range::find_if(_raw._columns, [&] (auto& column) {
        return column.name() == name;
    });
    if(it == _raw._columns.end()) {
        throw std::out_of_range(format("Cannot remove: column {} not found.", name));
    }
    without_column(it->name_as_text(), it->type, api::new_timestamp());
    _raw._columns.erase(it);
    return *this;
}

schema_builder& schema_builder::without_column(sstring name, api::timestamp_type timestamp) {
    return without_column(std::move(name), bytes_type, timestamp);
}

schema_builder& schema_builder::without_column(sstring name, data_type type, api::timestamp_type timestamp)
{
    auto ret = _raw._dropped_columns.emplace(name, schema::dropped_column{type, timestamp});
    if (!ret.second && ret.first->second.timestamp < timestamp) {
        ret.first->second.type = type;
        ret.first->second.timestamp = timestamp;
    }
    return *this;
}

schema_builder& schema_builder::rename_column(bytes from, bytes to)
{
    auto it = std::find_if(_raw._columns.begin(), _raw._columns.end(), [&] (auto& col) {
        return col.name() == from;
    });
    assert(it != _raw._columns.end());
    auto& def = *it;
    column_definition new_def(to, def.type, def.kind, def.component_index());
    _raw._columns.erase(it);
    return with_column(new_def);
}

schema_builder& schema_builder::alter_column_type(bytes name, data_type new_type)
{
    auto it = boost::find_if(_raw._columns, [&name] (auto& c) { return c.name() == name; });
    assert(it != _raw._columns.end());
    it->type = new_type;

    if (new_type->is_multi_cell()) {
        auto c_it = _raw._collections.find(name);
        assert(c_it != _raw._collections.end());
        c_it->second = new_type;
    }
    return *this;
}

schema_builder& schema_builder::mark_column_computed(bytes name, column_computation_ptr computation) {
    auto it = boost::find_if(_raw._columns, [&name] (const column_definition& c) { return c.name() == name; });
    assert(it != _raw._columns.end());
    it->set_computed(std::move(computation));

    return *this;
}

schema_builder& schema_builder::with_collection(bytes name, data_type type)
{
    _raw._collections.emplace(name, type);
    return *this;
}

schema_builder& schema_builder::with(compact_storage cs) {
    _compact_storage = cs;
    return *this;
}

schema_builder& schema_builder::with_version(table_schema_version v) {
    _version = v;
    return *this;
}

static const sstring default_partition_key_name = "key";
static const sstring default_clustering_name = "column";
static const sstring default_compact_value_name = "value";

schema_builder::default_names::default_names(const schema_builder& builder)
    : default_names(builder._raw)
{}

schema_builder::default_names::default_names(const schema::raw_schema& raw)
    : _raw(raw)
    , _partition_index(0)
    , _clustering_index(1)
    , _compact_index(0)
{}

sstring schema_builder::default_names::unique_name(const sstring& base, size_t& idx, size_t off) const {
    for (;;) {
        auto candidate = idx == 0 ? base : base + std::to_string(idx + off);
        ++idx;
        auto i = std::find_if(_raw._columns.begin(), _raw._columns.end(), [b = to_bytes(candidate)](const column_definition& c) {
            return c.name() == b;
        });
        if (i == _raw._columns.end()) {
            return candidate;
        }
    }
}

sstring schema_builder::default_names::partition_key_name() {
    // For compatibility sake, we call the first alias 'key' rather than 'key1'. This
    // is inconsistent with column alias, but it's probably not worth risking breaking compatibility now.
    return unique_name(default_partition_key_name, _partition_index, 1);
}

sstring schema_builder::default_names::clustering_name() {
    return unique_name(default_clustering_name, _clustering_index, 0);
}

sstring schema_builder::default_names::compact_value_name() {
    return unique_name(default_compact_value_name, _compact_index, 0);
}

void schema_builder::prepare_dense_schema(schema::raw_schema& raw) {
    auto is_dense = raw._is_dense;
    auto is_compound = raw._is_compound;
    auto is_compact_table = is_dense || !is_compound;

    if (is_compact_table) {
        auto count_kind = [&raw](column_kind kind) {
            return std::count_if(raw._columns.begin(), raw._columns.end(), [kind](const column_definition& c) {
                return c.kind == kind;
            });
        };

        default_names names(raw);

        if (is_dense) {
            auto regular_cols = count_kind(column_kind::regular_column);
            // In Origin, dense CFs always have at least one regular column
            if (regular_cols == 0) {
                raw._columns.emplace_back(to_bytes(names.compact_value_name()),
                                empty_type,
                                column_kind::regular_column, 0);
            } else if (regular_cols > 1) {
                throw exceptions::configuration_exception(
                                format("Expecting exactly one regular column. Found {:d}",
                                                regular_cols));
            }
        }
    }
}

schema_builder& schema_builder::with_view_info(utils::UUID base_id, sstring base_name, bool include_all_columns, sstring where_clause) {
    _view_info = raw_view_info(std::move(base_id), std::move(base_name), include_all_columns, std::move(where_clause));
    return *this;
}

schema_builder& schema_builder::with_index(const index_metadata& im) {
    _raw._indices_by_name.emplace(im.name(), im);
    return *this;
}

schema_builder& schema_builder::without_index(const sstring& name) {
    const auto& it = _raw._indices_by_name.find(name);
    if (it != _raw._indices_by_name.end()) {
        _raw._indices_by_name.erase(name);
    }
    return *this;
}

schema_builder& schema_builder::without_indexes() {
    _raw._indices_by_name.clear();
    return *this;
}

schema_ptr schema_builder::build() {
    schema::raw_schema new_raw = _raw; // Copy so that build() remains idempotent.

    if (_version) {
        new_raw._version = *_version;
    } else {
        new_raw._version = utils::UUID_gen::get_time_UUID();
    }

    if (new_raw._is_counter) {
        new_raw._default_validation_class = counter_type;
    }

    if (_compact_storage) {
        // Dense means that no part of the comparator stores a CQL column name. This means
        // COMPACT STORAGE with at least one columnAliases (otherwise it's a thrift "static" CF).
        auto clustering_key_size = std::count_if(new_raw._columns.begin(), new_raw._columns.end(), [](auto&& col) {
            return col.kind == column_kind::clustering_key;
        });
        new_raw._is_dense = (*_compact_storage == compact_storage::yes) && (clustering_key_size > 0);

        if (clustering_key_size == 0) {
            if (*_compact_storage == compact_storage::yes) {
                new_raw._is_compound = false;
            } else {
                new_raw._is_compound = true;
            }
        } else {
            if ((*_compact_storage == compact_storage::yes) && clustering_key_size == 1) {
                new_raw._is_compound = false;
            } else {
                new_raw._is_compound = true;
            }
        }
    }

    prepare_dense_schema(new_raw);
    return make_lw_shared<schema>(schema(new_raw, _view_info));
}

schema_ptr schema_builder::build(compact_storage cp) {
    return with(cp).build();
}

// Useful functions to manipulate the schema's comparator field
namespace cell_comparator {

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

static sstring compound_name(const schema& s) {
    sstring compound(_composite_str);

    compound += "(";
    if (s.clustering_key_size()) {
        for (auto &t : s.clustering_key_columns()) {
            compound += t.type->name() + ",";
        }
    }

    if (!s.is_dense()) {
        compound += s.regular_column_name_type()->name() + ",";
    }

    if (!s.collections().empty()) {
        compound += _collection_str;
        compound += "(";
        for (auto& c : s.collections()) {
            compound += format("{}:{},", to_hex(c.first), c.second->name());
        }
        compound.back() = ')';
        compound += ",";
    }
    // last one will be a ',', just replace it.
    compound.back() = ')';
    return compound;
}

sstring to_sstring(const schema& s) {
    if (s.is_compound()) {
        return compound_name(s);
    } else if (s.clustering_key_size() == 1) {
        assert(s.is_dense() || s.is_static_compact_table());
        return s.clustering_key_columns().front().type->name();
    } else {
        return s.regular_column_name_type()->name();
    }
}

bool check_compound(sstring comparator) {
    static sstring compound(_composite_str);
    return comparator.compare(0, compound.size(), compound) == 0;
}

void read_collections(schema_builder& builder, sstring comparator)
{
    // The format of collection entries in the comparator is:
    // org.apache.cassandra.db.marshal.ColumnToCollectionType(<name1>:<type1>, ...)

    auto find_closing_parenthesis = [] (sstring_view str, size_t start) {
        auto pos = start;
        auto nest_level = 0;
        do {
            pos = str.find_first_of("()", pos);
            if (pos == sstring::npos) {
                throw marshal_exception("read_collections - can't find any parentheses");
            }
            if (str[pos] == ')') {
                nest_level--;
            } else if (str[pos] == '(') {
                nest_level++;
            }
            pos++;
        } while (nest_level > 0);
        return pos - 1;
    };

    auto collection_str_length = strlen(_collection_str);

    auto pos = comparator.find(_collection_str);
    if (pos == sstring::npos) {
        return;
    }
    pos += collection_str_length + 1;
    while (pos < comparator.size()) {
        size_t end = comparator.find('(', pos);
        if (end == sstring::npos) {
            throw marshal_exception("read_collections - open parenthesis not found");
        }
        end = find_closing_parenthesis(comparator, end) + 1;

        auto colon = comparator.find(':', pos);
        if (colon == sstring::npos || colon > end) {
            throw marshal_exception("read_collections - colon not found");
        }

        auto name = from_hex(sstring_view(comparator.c_str() + pos, colon - pos));

        colon++;
        auto type_str = sstring_view(comparator.c_str() + colon, end - colon);
        auto type = db::marshal::type_parser::parse(type_str);

        builder.with_collection(name, type);

        if (end < comparator.size() && comparator[end] == ',') {
            pos = end + 1;
        } else if (end < comparator.size() && comparator[end] == ')') {
            pos = sstring::npos;
        } else {
            throw marshal_exception("read_collections - invalid collection format");
        }
    }
}

}

schema::const_iterator
schema::regular_begin() const {
    return regular_columns().begin();
}

schema::const_iterator
schema::regular_end() const {
    return regular_columns().end();
}

struct column_less_comparator {
    bool operator()(const column_definition& def, const bytes& name) {
        return def.name() < name;
    }
    bool operator()(const bytes& name, const column_definition& def) {
        return name < def.name();
    }
};

schema::const_iterator
schema::regular_lower_bound(const bytes& name) const {
    return boost::lower_bound(regular_columns(), name, column_less_comparator());
}

schema::const_iterator
schema::regular_upper_bound(const bytes& name) const {
    return boost::upper_bound(regular_columns(), name, column_less_comparator());
}

schema::const_iterator
schema::static_begin() const {
    return static_columns().begin();
}

schema::const_iterator
schema::static_end() const {
    return static_columns().end();
}

schema::const_iterator
schema::static_lower_bound(const bytes& name) const {
    return boost::lower_bound(static_columns(), name, column_less_comparator());
}

schema::const_iterator
schema::static_upper_bound(const bytes& name) const {
    return boost::upper_bound(static_columns(), name, column_less_comparator());
}
data_type
schema::column_name_type(const column_definition& def) const {
    if (def.kind == column_kind::regular_column) {
        return _raw._regular_column_name_type;
    }
    return utf8_type;
}

const column_definition&
schema::regular_column_at(column_id id) const {
    if (id > regular_columns_count()) {
        throw std::out_of_range("column_id");
    }
    return _raw._columns.at(column_offset(column_kind::regular_column) + id);
}

const column_definition&
schema::clustering_column_at(column_id id) const {
    if (id >= clustering_key_size()) {
        throw std::out_of_range(format("clustering column id {:d} >= {:d}", id, clustering_key_size()));
    }
    return _raw._columns.at(column_offset(column_kind::clustering_key) + id);
}

const column_definition&
schema::static_column_at(column_id id) const {
    if (id > static_columns_count()) {
        throw std::out_of_range("column_id");
    }
    return _raw._columns.at(column_offset(column_kind::static_column) + id);
}

bool
schema::is_last_partition_key(const column_definition& def) const {
    return &_raw._columns.at(partition_key_size() - 1) == &def;
}

bool
schema::has_static_columns() const {
    return !static_columns().empty();
}

column_count_type
schema::columns_count(column_kind kind) const {
    switch (kind) {
    case column_kind::partition_key:
        return partition_key_size();
    case column_kind::clustering_key:
        return clustering_key_size();
    case column_kind::static_column:
        return static_columns_count();
    case column_kind::regular_column:
        return regular_columns_count();
    default:
        std::abort();
    }
}
column_count_type
schema::partition_key_size() const {
    return column_offset(column_kind::clustering_key);
}

schema::const_iterator_range_type
schema::partition_key_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::partition_key)
            , _raw._columns.begin() + column_offset(column_kind::clustering_key));
}

schema::const_iterator_range_type
schema::clustering_key_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::clustering_key)
            , _raw._columns.begin() + column_offset(column_kind::static_column));
}

schema::const_iterator_range_type
schema::static_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::static_column)
            , _raw._columns.begin() + column_offset(column_kind::regular_column));
}

schema::const_iterator_range_type
schema::regular_columns() const {
    return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::regular_column)
            , _raw._columns.end());
}

schema::select_order_range schema::all_columns_in_select_order() const {
    auto is_static_compact_table = this->is_static_compact_table();
    auto no_non_pk_columns = is_compact_table()
                    // Origin: && CompactTables.hasEmptyCompactValue(this);
                    && regular_columns_count() == 1
                    && [](const column_definition& c) {
        // We use empty_type now to match origin, but earlier incarnations
        // set name empty instead. check either.
        return c.type == empty_type || c.name().empty();
    }(regular_column_at(0));
    auto pk_range = const_iterator_range_type(_raw._columns.begin(),
                    _raw._columns.begin() + (is_static_compact_table ?
                                    column_offset(column_kind::clustering_key) :
                                    column_offset(column_kind::static_column)));
    auto ck_v_range = no_non_pk_columns ? static_columns()
                                        : const_iterator_range_type(static_columns().begin(), all_columns().end());
    return boost::range::join(pk_range, ck_v_range);
}

uint32_t
schema::position(const column_definition& column) const {
    if (column.is_primary_key()) {
        return column.id;
    }
    return clustering_key_size();
}

std::optional<index_metadata> schema::find_index_noname(const index_metadata& target) const {
    const auto& it = boost::find_if(_raw._indices_by_name, [&] (auto&& e) {
        return e.second.equals_noname(target);
    });
    if (it != _raw._indices_by_name.end()) {
        return it->second;
    }
    return {};
}

std::vector<index_metadata> schema::indices() const {
    return boost::copy_range<std::vector<index_metadata>>(_raw._indices_by_name | boost::adaptors::map_values);
}

const std::unordered_map<sstring, index_metadata>& schema::all_indices() const {
    return _raw._indices_by_name;
}

bool schema::has_index(const sstring& index_name) const {
    return _raw._indices_by_name.count(index_name) > 0;
}

std::vector<sstring> schema::index_names() const {
    return boost::copy_range<std::vector<sstring>>(_raw._indices_by_name | boost::adaptors::map_keys);
}

data_type schema::make_legacy_default_validator() const {
    return _raw._default_validation_class;
}

bool schema::is_synced() const {
    return _registry_entry && _registry_entry->is_synced();
}

bool schema::equal_columns(const schema& other) const {
    return boost::equal(all_columns(), other.all_columns());
}

raw_view_info::raw_view_info(utils::UUID base_id, sstring base_name, bool include_all_columns, sstring where_clause)
        : _base_id(std::move(base_id))
        , _base_name(std::move(base_name))
        , _include_all_columns(include_all_columns)
        , _where_clause(where_clause)
{ }

column_computation_ptr column_computation::deserialize(bytes_view raw) {
    return deserialize(json::to_json_value(sstring(raw.begin(), raw.end())));
}

column_computation_ptr column_computation::deserialize(const Json::Value& parsed) {
    if (!parsed.isObject()) {
        throw std::runtime_error(format("Invalid column computation value: {}", parsed.toStyledString()));
    }
    Json::Value type_json = parsed.get("type", Json::Value());
    if (!type_json.isString()) {
        throw std::runtime_error(format("Type {} is not convertible to string", type_json.toStyledString()));
    }
    sstring type = type_json.asString();
    if (type == "token") {
        return std::make_unique<token_column_computation>();
    }
    throw std::runtime_error(format("Incorrect column computation type {} found when parsing {}", type, parsed.toStyledString()));
}

bytes token_column_computation::serialize() const {
    Json::Value serialized(Json::objectValue);
    serialized["type"] = Json::Value("token");
    return to_bytes(json::to_sstring(serialized));
}

bytes_opt token_column_computation::compute_value(const schema& schema, const partition_key& key, const clustering_row& row) const {
    dht::i_partitioner& partitioner = dht::global_partitioner();
    return partitioner.token_to_bytes(partitioner.get_token(schema, key));
}

bool operator==(const raw_view_info& x, const raw_view_info& y) {
    return x._base_id == y._base_id
        && x._base_name == y._base_name
        && x._include_all_columns == y._include_all_columns
        && x._where_clause == y._where_clause;
}

std::ostream& operator<<(std::ostream& os, const raw_view_info& view) {
    os << "ViewInfo{";
    os << "baseTableId=" << view._base_id;
    os << ", baseTableName=" << view._base_name;
    os << ", includeAllColumns=" << view._include_all_columns;
    os << ", whereClause=" << view._where_clause;
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const view_ptr& view) {
    return view ? os << *view : os << "null";
}

schema_mismatch_error::schema_mismatch_error(table_schema_version expected, const schema& access)
    : std::runtime_error(fmt::format("Attempted to deserialize schema-dependent object of version {} using {}.{} {}",
        expected, access.ks_name(), access.cf_name(), access.version()))
{ }
