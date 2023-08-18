#ifndef CONVERSIONS_H_
#define CONVERSIONS_H_

#include <arrow/result.h> // for Result
#include <stddef.h>       // for size_t
#include <stdint.h>       // for uint64_t
#include <string>

#include <memory>        // for shared_ptr, make_shared
#include <string_view>   // for string_view
#include <unordered_map> // for unordered_map
#include <vector>        // for vector
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

extern const std::string AsciiType, BooleanType, ByteType, BytesType, DateType, DecimalType, DoubleType, DurationType,
    FloatType, InetAddressType, Int32Type, IntegerType, LexicalUUIDType, LongType, ShortType, SimpleDateType, TimeType,
    TimeUUIDType, TimestampType, UTF8Type, UUIDType, CompositeType, ListType, MapType, SetType, TupleType, ReversedType;

}

static const uint64_t TIMESTAMP_EPOCH = 1442880000000000;
static const uint64_t DELETION_TIME_EPOCH = 1442880000;
static const uint64_t LOCAL_DELETION_TIME_NULL = 0x7fffffff;
static const uint64_t MARKED_FOR_DELETE_AT_NULL = 0x8000000000000000;

struct cassandra_type
{
    std::string_view cql_name;
    size_t fixed_len;
    std::shared_ptr<arrow::DataType> arrow_type;
    bool cudf_supported = true;
};

// used for parsing complex types
struct node
{
    std::string_view str;
    std::shared_ptr<std::vector<std::shared_ptr<node>>> children;
    node(std::string_view str_) : str(str_), children(std::make_shared<std::vector<std::shared_ptr<node>>>())
    {
    }
};

extern const std::unordered_map<std::string_view, cassandra_type> type_info;

// order matters and is used in sstable_data.ksy to determine how child cells
// are encoded
extern const std::vector<std::string_view> multi_cell_types;

size_t get_col_size(std::string_view coltype, kaitai::kstream *ks);
bool is_multi_cell(std::string_view coltype);
bool is_vector(std::string_view coltype);
std::string_view get_child_type(std::string_view type);
void get_map_child_types(std::string_view type, std::string_view *key_type, std::string_view *value_type);

bool is_reversed(std::string_view type);
bool is_composite(std::string_view type);
bool is_list(std::string_view type);
bool is_map(std::string_view type);
bool is_set(std::string_view type);
bool is_tuple(std::string_view type);
bool is_uuid(std::string_view type);
bool is_float(std::string_view type);

struct get_arrow_type_options
{
    std::shared_ptr<arrow::DataType> replace_with{nullptr};
    bool for_cudf{false};
};

void splitTypeAndLength(std::string_view& input, int& number);

/**
 * @brief Transform the given Cassandra type string into an arrow DataType
 *
 * @param type the Cassandra type as a string
 * @param options whether to replace leaves with a different type, or whether to make accomodations for cuDF types
 * @param needs_second will set this to true if options.for_cudf is set and a UUID type is encountered while parsing
 * @return arrow::Result<std::shared_ptr<arrow::DataType>>
 */
arrow::Result<std::shared_ptr<arrow::DataType>> get_arrow_type(
    std::string_view type, const get_arrow_type_options &options = get_arrow_type_options{},
    bool *needs_second = nullptr);

/**
 * @brief Parse a Cassandra type, represented as a string, into an abstract tree.
 *
 * @param type the Cassandra type as a string, e.g.
 * org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)
 * @return arrow::Result<std::shared_ptr<node>>
 */
arrow::Result<std::shared_ptr<node>> parse_nested_type(std::string_view type);

} // namespace conversions

#endif
