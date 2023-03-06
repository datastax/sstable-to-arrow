#include "conversions.h"
#include <algorithm>                            // for copy, copy_backward
#include <arrow/status.h>                       // for Status
#include <arrow/type_fwd.h>                     // for field, int64, fixed_size_binary, int32
#include <boost/algorithm/string/predicate.hpp> // for starts_with, ends_with
#include <ext/alloc_traits.h>                   // for __alloc_traits<>::value_type
#include <stack>                                // for stack
#include <stdexcept>                            // for runtime_error, out_of_range
#include <string>                               // for operator+, to_string, string, char_traits
#include <utility>                              // for pair
//#include <iostream> 

#include "vint.h" // for vint_t
namespace arrow
{
class DataType;
}
namespace kaitai
{
class kstream;
}

namespace conversions
{

namespace types
{
#define DEFINE_TYPE(name) const std::string name##Type = "org.apache.cassandra.db.marshal." #name "Type"

DEFINE_TYPE(Ascii);
DEFINE_TYPE(Boolean);
DEFINE_TYPE(Byte);
DEFINE_TYPE(Bytes);
DEFINE_TYPE(Date);
DEFINE_TYPE(Decimal);
DEFINE_TYPE(Double);
DEFINE_TYPE(Duration);
DEFINE_TYPE(Float);
DEFINE_TYPE(InetAddress);
DEFINE_TYPE(Int32);
DEFINE_TYPE(Integer);
DEFINE_TYPE(LexicalUUID);
DEFINE_TYPE(Long);
DEFINE_TYPE(Short);
DEFINE_TYPE(SimpleDate);
DEFINE_TYPE(Time);
DEFINE_TYPE(TimeUUID);
DEFINE_TYPE(Timestamp);
DEFINE_TYPE(UTF8);
DEFINE_TYPE(UUID);
DEFINE_TYPE(Composite);
DEFINE_TYPE(List);
DEFINE_TYPE(Map);
DEFINE_TYPE(Set);
DEFINE_TYPE(Tuple);
DEFINE_TYPE(Reversed);

#undef DEFINE_TYPE

} // namespace types

arrow::FieldVector decimal_fields{arrow::field("scale", arrow::int32()), arrow::field("val", arrow::binary())};
arrow::FieldVector inet_fields{arrow::field("ipv4", arrow::int32()), arrow::field("ipv6", arrow::int64())};

const std::vector<std::string_view> multi_cell_types{types::ListType, types::MapType, types::SetType};

// see https://docs.rapids.ai/api/cudf/stable/basics.html
// for a list of types that cudf supports
const std::unordered_map<std::string_view, cassandra_type> type_info{
    {types::AsciiType, {"ascii", 0, arrow::utf8()}},         // ascii
    {types::BooleanType, {"boolean", 1, arrow::boolean()}},  // boolean
    {types::ByteType, {"tinyint", 0, arrow::int8()}},        // tinyint
    {types::BytesType, {"blob", 0, arrow::binary(), false}}, // blob
    // {"org.apache.cassandra.db.marshal.CompositeType", { "", 0 }},
    // {"org.apache.cassandra.db.marshal.CounterColumnType", { "", 0 }}, //
    // extends "long"
    {types::DateType, {"", 8, arrow::timestamp(arrow::TimeUnit::MILLI)}},              // old version of TimestampType
    {types::DecimalType, {"decimal", 0, arrow::struct_(decimal_fields)}},              // decimal, custom implementation
    {types::DoubleType, {"double", 8, arrow::float64()}},                              // double
    {types::DurationType, {"duration", 0, arrow::fixed_size_list(arrow::int64(), 3)}}, // duration
    // {"org.apache.cassandra.db.marshal.DynamicCompositeType", { "", 0 }},
    // {"org.apache.cassandra.db.marshal.EmptyType", { "", 0 }},
    {types::FloatType, {"float", 4, arrow::float32()}}, // float
    // {"org.apache.cassandra.db.marshal.FrozenType", { "", 0 }},
    {types::InetAddressType, {"inet", 0, arrow::dense_union(inet_fields)}}, // inet
    {types::Int32Type, {"int", 4, arrow::int32()}},                         // int
    {types::IntegerType, {"varint", 0, arrow::int64()}},                    // varint
    {types::LexicalUUIDType, {"", 16, arrow::fixed_size_binary(16), false}},
    {types::LongType, {"bigint", 8, arrow::int64()}}, // bigint
    // {"org.apache.cassandra.db.marshal.PartitionerDefinedOrder", { "", 0 }},
    // // not for user-defined
    // https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/marshal/ReversedType.java
    {types::ShortType, {"smallint", 0, arrow::int16()}},                  // smallint
    {types::SimpleDateType, {"date", 0, arrow::date32()}},                // date, represented as 32-bit unsigned
    {types::TimeType, {"time", 0, arrow::time64(arrow::TimeUnit::NANO)}}, // time
    {types::TimeUUIDType, {"timeuuid", 16, arrow::fixed_size_binary(16), false}},       // timeuuid
    {types::TimestampType, {"timestamp", 8, arrow::timestamp(arrow::TimeUnit::MILLI)}}, // timestamp
    // {"org.apache.cassandra.db.marshal.TupleType", { "", 0 }},
    {types::UTF8Type, {"text", 0, arrow::utf8()}},                        // text, varchar
    {types::UUIDType, {"uuid", 16, arrow::fixed_size_binary(16), false}}, // uuid
    // {"org.apache.cassandra.db.marshal.UserType", { "", 0 }},
};

size_t get_col_size(std::string_view coltype, kaitai::kstream *ks)
{
    if (coltype.rfind(types::ReversedType, 0) == 0)
        return get_col_size(get_child_type(coltype), ks);
    if (is_multi_cell(coltype))
    {
        // it seems like children cells of a complex cell have their
        // size marked as a varint instead of the expected value
        // TODO confirm this is the case
        long long len = vint_t(ks).val();
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
    //std::cout << "coltype: " << coltype << "\n" ;
    //std::cout << "size: " << len << "\n" ;
    return len;
}

std::string_view get_child_type(std::string_view type)
{
    size_t start = type.find('(') + 1;
    return std::string_view(type.data() + start, type.rfind(')') - start);
}

void get_map_child_types(std::string_view type, std::string_view *key_type, std::string_view *value_type)
{
    const int sep_idx = type.find(',');
    size_t key_len = sep_idx - (types::MapType.size() + 1);
    size_t value_len = type.size() - 1 - (sep_idx + 1);
    *key_type = std::string_view(type.data() + types::MapType.size() + 1, key_len);
    *value_type = std::string_view(type.data() + sep_idx + 1, value_len);
}

/**
 * Checks if the currently selected cell has multiple child cells (usually a
 * collection like a list, map, set, etc) These are usually referred to as
 * complex cells
 */
bool is_multi_cell(std::string_view coltype)
{
    if (is_reversed(coltype))
        return is_multi_cell(get_child_type(coltype));

    for (std::string_view complex_type : multi_cell_types)
        if (coltype.rfind(complex_type, 0) == 0)
            return true;
    return false;
}

bool is_list(std::string_view type)
{
    return boost::starts_with(type, types::ListType);
}
bool is_map(std::string_view type)
{
    return boost::starts_with(type, types::MapType);
}
bool is_set(std::string_view type)
{
    return boost::starts_with(type, types::SetType);
}
bool is_reversed(std::string_view type)
{
    return boost::starts_with(type, types::ReversedType);
}
bool is_composite(std::string_view type)
{
    return boost::starts_with(type, types::CompositeType);
}
bool is_tuple(std::string_view type)
{
    return boost::starts_with(type, types::TupleType);
}
bool is_uuid(std::string_view type)
{
    return boost::ends_with(type, "UUIDType");
}

arrow::Result<std::shared_ptr<arrow::DataType>> get_arrow_type(std::string_view type,
                                                               const get_arrow_type_options &options,
                                                               bool *needs_second)
{
    try
    {
        auto _t = type_info.at(type);
        // doesn't throw means type is a "primitive"
        if (options.replace_with != nullptr)
            return options.replace_with;
        if (options.for_cudf && is_uuid(type))
        {
            if (needs_second)
                *needs_second = true;
            return arrow::uint64();
        }
        if (options.for_cudf && !_t.cudf_supported)
            return arrow::utf8(); // pass unsupported types as hexadecimal strings
        return _t.arrow_type;
    }

    // if this type doesn't exist in type_info
    catch (const std::out_of_range &err)
    {
        auto tree = *parse_nested_type(type);

        if (is_reversed(type))
            return get_arrow_type(get_child_type(type), options, needs_second);
        else if (is_map(type))
        {
            // we keep the key type but recursively replace the value type with
            // timestamps
            const auto &key_type_str = tree->children->front()->str;
            const auto &item_type_str = tree->children->back()->str;
            if (options.for_cudf && is_uuid(key_type_str) || is_uuid(item_type_str))
                return arrow::Status::NotImplemented("UUIDs are currently not supported inside collections");
            ARROW_ASSIGN_OR_RAISE(const auto &key_type, get_arrow_type(key_type_str, options, needs_second));
            ARROW_ASSIGN_OR_RAISE(const auto &item_type, get_arrow_type(item_type_str, options, needs_second));
            return arrow::map(key_type, item_type);
        }
        else if (is_set(type) || is_list(type)) // TODO currently treating sets and lists identically
        {
            const auto &child_str = get_child_type(type);
            if (options.for_cudf && is_uuid(child_str))
                return arrow::Status::NotImplemented("UUIDs are currently not supported inside collections");
            ARROW_ASSIGN_OR_RAISE(const auto &child_type, get_arrow_type(child_str, options, needs_second));
            return arrow::list(child_type);
        }
        else if (is_composite(type))
        {
            arrow::FieldVector vec;
            for (size_t i = 0; i < tree->children->size(); ++i)
            {
                // uuids are fine inside a struct
                ARROW_ASSIGN_OR_RAISE(const auto &member_type,
                                      get_arrow_type((*tree->children)[i]->str, options, needs_second));
                vec.push_back(arrow::field(std::string((*tree->children)[i]->str), member_type));
            }
            return arrow::struct_(vec);
        }
    }

    return arrow::Status::Invalid("type not found or supported when getting arrow type: " + std::string(type));
}

arrow::Result<std::shared_ptr<node>> parse_nested_type(std::string_view cass_type)
{
    std::stack<std::shared_ptr<node>> stack;
    size_t prev_start = 0;
    char prev = '\0';
    for (size_t j = 0; j < cass_type.size(); ++j)
    {
        if (cass_type[j] == ' ' || cass_type[j] == '\t' || cass_type[j] == '\n')
            continue;

        if (cass_type[j] == '(')
        {
            std::string_view str(&cass_type[prev_start], j - prev_start);
            stack.push(std::make_shared<node>(str));
            prev_start = j + 1;
        }

        if (cass_type[j] == ',' || cass_type[j] == ')')
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
                auto parent = stack.top(); // reference to current parent cass_type
                const std::string_view str(&cass_type[prev_start], j - prev_start);
                parent->children->push_back(std::make_shared<node>(str));
            }
            prev_start = j + 1;
        }

        // finish parsing the cass_type
        if (cass_type[j] == ')')
        {
            auto token = stack.top(); // reference to current parent cass_type
            if (stack.size() == 1)
                return stack.top();
            stack.pop();
            prev_start = j + 1;
        }

        prev = cass_type[j];
    }

    return std::make_shared<node>(cass_type);
}

} // namespace conversions
