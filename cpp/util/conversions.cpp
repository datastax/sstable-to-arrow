#include "conversions.h"

namespace conversions
{

arrow::FieldVector decimal_fields{
    arrow::field("scale", arrow::int32()), arrow::field("val", arrow::binary())};
arrow::FieldVector inet_fields{
    arrow::field("ipv4", arrow::int32()), arrow::field("ipv6", arrow::int64())};

const std::string_view compositetype = "org.apache.cassandra.db.marshal.CompositeType";
const std::string_view listtype = "org.apache.cassandra.db.marshal.ListType";
const std::string_view maptype = "org.apache.cassandra.db.marshal.MapType";
const std::string_view settype = "org.apache.cassandra.db.marshal.SetType";
const std::string_view tupletype = "org.apache.cassandra.db.marshal.TupleType";       // TODO
const std::string_view reversedtype = "org.apache.cassandra.db.marshal.ReversedType"; // TODO

const std::vector<std::string_view> multi_cell_types{
    conversions::listtype,
    conversions::maptype,
    conversions::settype};

// see https://docs.rapids.ai/api/cudf/stable/basics.html
// for a list of types that cudf supports
const std::unordered_map<std::string_view, cassandra_type> type_info{
    {"org.apache.cassandra.db.marshal.AsciiType", {"ascii", 0, arrow::utf8()}},        // ascii
    {"org.apache.cassandra.db.marshal.BooleanType", {"boolean", 1, arrow::boolean()}}, // boolean
    {"org.apache.cassandra.db.marshal.ByteType", {"tinyint", 0, arrow::int8()}},       // tinyint
    {"org.apache.cassandra.db.marshal.BytesType", {"blob", 0, arrow::binary(), false}},       // blob
    // {"org.apache.cassandra.db.marshal.CompositeType", { "", 0 }},
    // {"org.apache.cassandra.db.marshal.CounterColumnType", { "", 0 }}, // extends "long"
    {"org.apache.cassandra.db.marshal.DateType", {"", 8, arrow::timestamp(arrow::TimeUnit::MILLI)}},              // old version of TimestampType
    {"org.apache.cassandra.db.marshal.DecimalType", {"decimal", 0, arrow::struct_(decimal_fields)}},              // decimal, custom implementation
    {"org.apache.cassandra.db.marshal.DoubleType", {"double", 8, arrow::float64()}},                              // double
    {"org.apache.cassandra.db.marshal.DurationType", {"duration", 0, arrow::fixed_size_list(arrow::int64(), 3)}}, // duration
    // {"org.apache.cassandra.db.marshal.DynamicCompositeType", { "", 0 }},
    // {"org.apache.cassandra.db.marshal.EmptyType", { "", 0 }},
    {"org.apache.cassandra.db.marshal.FloatType", {"float", 4, arrow::float32()}}, // float
    // {"org.apache.cassandra.db.marshal.FrozenType", { "", 0 }},
    {"org.apache.cassandra.db.marshal.InetAddressType", {"inet", 0, arrow::dense_union(inet_fields)}}, // inet
    {"org.apache.cassandra.db.marshal.Int32Type", {"int", 4, arrow::int32()}},                         // int
    {"org.apache.cassandra.db.marshal.IntegerType", {"varint", 0, arrow::int64()}},                    // varint
    {"org.apache.cassandra.db.marshal.LexicalUUIDType", {"", 16, arrow::fixed_size_binary(16), false}},
    // TODO ListType
    {"org.apache.cassandra.db.marshal.LongType", {"bigint", 8, arrow::int64()}}, // bigint
    // TODO MapType
    // {"org.apache.cassandra.db.marshal.PartitionerDefinedOrder", { "", 0 }}, // not for user-defined
    // https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/marshal/ReversedType.java
    // TODO SetType
    {"org.apache.cassandra.db.marshal.ShortType", {"smallint", 0, arrow::int16()}},                                // smallint
    {"org.apache.cassandra.db.marshal.SimpleDateType", {"date", 0, arrow::date32()}},                              // date, represented as 32-bit unsigned
    {"org.apache.cassandra.db.marshal.TimeType", {"time", 0, arrow::time64(arrow::TimeUnit::NANO)}},               // time
    {"org.apache.cassandra.db.marshal.TimeUUIDType", {"timeuuid", 16, arrow::fixed_size_binary(16), false}},              // timeuuid
    {"org.apache.cassandra.db.marshal.TimestampType", {"timestamp", 8, arrow::timestamp(arrow::TimeUnit::MILLI)}}, // timestamp
    // {"org.apache.cassandra.db.marshal.TupleType", { "", 0 }},
    {"org.apache.cassandra.db.marshal.UTF8Type", {"text", 0, arrow::utf8()}},                 // text, varchar
    {"org.apache.cassandra.db.marshal.UUIDType", {"uuid", 16, arrow::fixed_size_binary(16), false}}, // uuid
    // {"org.apache.cassandra.db.marshal.UserType", { "", 0 }},
};

long long get_col_size(const std::string_view &coltype, kaitai::kstream *ks)
{
    DEBUG_ONLY(std::cout << "getting col size of " << coltype << '\n');
    if (coltype.rfind(reversedtype, 0) == 0)
        return get_col_size(get_child_type(coltype), ks);
    if (is_multi_cell(coltype))
    {
        // it seems like children cells of a complex cell have their
        // size marked as a varint instead of the expected value
        // TODO confirm this is the case
        long long len = vint_t(ks).val();
        DEBUG_ONLY(std::cout << "length of child cell: " << len << '\n');
        return len;
    }

    // check if this data type has a fixed length
    auto it = type_info.find(coltype);
    if (it == type_info.end())
        throw std::runtime_error("unrecognized type when getting col size: " + std::string(coltype));
    long long len;
    if (it->second.fixed_len != 0)
        len = it->second.fixed_len;
    // otherwise read the length as a varint
    else
        len = vint_t(ks).val();
    DEBUG_ONLY(std::cout << "length: " << len << '\n');
    return len;
}

std::string_view get_child_type(const std::string_view &type)
{
    size_t start = type.find('(') + 1;
    return std::string_view(
        type.data() + start,
        type.rfind(')') - start);
}

void get_map_child_types(const std::string_view &type, std::string_view *key_type, std::string_view *value_type)
{
    const int sep_idx = type.find(',');
    size_t key_len = sep_idx - (maptype.size() + 1);
    size_t value_len = type.size() - 1 - (sep_idx + 1);
    *key_type = std::string_view(type.data() + maptype.size() + 1, key_len);
    *value_type = std::string_view(type.data() + sep_idx + 1, value_len);
}

/**
     * Checks if the currently selected cell has multiple child cells (usually a collection like a list, map, set, etc)
     * These are usually referred to as complex cells
     */
bool is_multi_cell(const std::string_view &coltype)
{
    if (is_reversed(coltype))
        return is_multi_cell(get_child_type(coltype));

    for (const std::string_view &complex_type : multi_cell_types)
        if (coltype.rfind(complex_type, 0) == 0)
            return true;
    return false;
}

#define IS_TYPE_WITH_PARAMETERS(name)            \
    bool is_##name(const std::string_view &type) \
    {                                            \
        return type.rfind(name##type, 0) == 0;   \
    }

IS_TYPE_WITH_PARAMETERS(list)
IS_TYPE_WITH_PARAMETERS(map)
IS_TYPE_WITH_PARAMETERS(set)
IS_TYPE_WITH_PARAMETERS(reversed)
IS_TYPE_WITH_PARAMETERS(composite)
IS_TYPE_WITH_PARAMETERS(tuple)

#undef IS_TYPE_WITH_PARAMETERS

std::shared_ptr<arrow::DataType> get_arrow_type(const std::string_view &type, const get_arrow_type_options &options)
{
    try
    {
        auto _t = type_info.at(type);
        // doesn't throw means type is a "primitive"
        if (options.replace_with != nullptr)
            return options.replace_with;
        if (global_flags.for_cudf && !_t.cudf_supported)
            return arrow::utf8(); // pass unsupported types as hexadecimal strings
        return _t.arrow_type;
    }
    catch (const std::out_of_range &err)
    {
        // pass
    }

    auto tree = *parse_nested_type(type);

    if (is_reversed(type))
        return get_arrow_type(get_child_type(type), options);
    else if (is_map(type))
        // we keep the key type but recursively replace the value type with timestamps
        return arrow::map(get_arrow_type(tree->children->front()->str), get_arrow_type(tree->children->back()->str, options));
    else if (is_set(type) || is_list(type)) // TODO currently treating sets and lists identically
        return arrow::list(get_arrow_type(get_child_type(type), options));
    else if (is_composite(type))
    {
        arrow::FieldVector vec;
        for (int i = 0; i < tree->children->size(); ++i)
            vec.push_back(arrow::field(std::string((*tree->children)[i]->str), get_arrow_type((*tree->children)[i]->str, options)));
        return arrow::struct_(vec);
    }

    throw std::runtime_error("type not found or supported when getting arrow type: " + std::string(type));
}

arrow::Result<std::shared_ptr<node>> parse_nested_type(const std::string_view &type)
{
    std::stack<std::shared_ptr<node>> stack;
    size_t prev_start = 0;
    char prev = '\0';
    for (size_t j = 0; j < type.size(); ++j)
    {
        if (type[j] == ' ' || type[j] == '\t' || type[j] == '\n')
            continue;

        if (type[j] == '(')
        {
            std::string_view str(&type[prev_start], j - prev_start);
            stack.push(std::make_shared<node>(str));
            prev_start = j + 1;
        }

        if (type[j] == ',' || type[j] == ')')
        {
            if (prev == ')')
            {
                auto child = stack.top();
                stack.pop();
                auto parent = stack.top();
                parent->children->push_back(child);
            }
            else
            {
                auto parent = stack.top(); // reference to current parent type
                const std::string_view str(&type[prev_start], j - prev_start);
                parent->children->push_back(std::make_shared<node>(str));
            }
            prev_start = j + 1;
        }

        // finish parsing the type
        if (type[j] == ')')
        {
            auto token = stack.top(); // reference to current parent type
            if (stack.size() == 1)
                return stack.top();
            stack.pop();
            prev_start = j + 1;
        }

        prev = type[j];
    }
}

} // namespace conversions
