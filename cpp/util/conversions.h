#ifndef CONVERSIONS_H_
#define CONVERSIONS_H_

#include <kaitai/kaitaistream.h>
#include <string_view>
#include <unordered_map>
#include <arrow/api.h>
#include <vector>
#include <stack>
#include "vint.h"
#include "opts.h"

namespace conversions
{

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
    node(std::string_view str_)
        : str(str_), children(std::make_shared<std::vector<std::shared_ptr<node>>>()) {}
};

extern const std::unordered_map<std::string_view, cassandra_type> type_info;

// special types that take type parameters
extern const std::string_view compositetype, listtype, maptype, settype, tupletype, reversedtype;

// order matters and is used in sstable_data.ksy to determine how child cells
// are encoded
extern const std::vector<std::string_view> multi_cell_types;

size_t get_col_size(std::string_view coltype, kaitai::kstream *ks);
bool is_multi_cell(std::string_view coltype);
std::string_view get_child_type(std::string_view type);
void get_map_child_types(std::string_view type, std::string_view *key_type, std::string_view *value_type);

bool is_reversed(std::string_view type);
bool is_composite(std::string_view type);
bool is_list(std::string_view type);
bool is_map(std::string_view type);
bool is_set(std::string_view type);
bool is_tuple(std::string_view type);

struct get_arrow_type_options {
    std::shared_ptr<arrow::DataType> replace_with{nullptr};
};

std::shared_ptr<arrow::DataType> get_arrow_type(std::string_view type, const get_arrow_type_options &options = get_arrow_type_options{});
arrow::Result<std::shared_ptr<node>> parse_nested_type(std::string_view type);

} // namespace conversions

#endif
