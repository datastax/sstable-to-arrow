#ifndef CONVERSIONS_H_
#define CONVERSIONS_H_

#include <kaitai/kaitaistream.h>
#include <string_view>
#include <string_view>
#include <unordered_map>
#include <arrow/api.h>
#include <vector>
#include <stack>
#include "timer.h"
#include "vint.h"

namespace conversions
{
    struct cassandra_type
    {
        std::string_view cql_name;
        size_t fixed_len;
        std::shared_ptr<arrow::DataType> arrow_type;
    };

    struct node
    {
        std::string_view str;
        std::shared_ptr<std::vector<std::shared_ptr<struct node>>> children;
        node(const std::string_view &str_);
    };

    // special types that take type parameters
    extern const std::string_view compositetype, listtype, maptype, settype, tupletype, reversedtype;

    // order matters and is used in sstable_data.ksy to determine how child cells
    // are encoded
    extern const std::vector<std::string_view> multi_cell_types;

    long long get_col_size(const std::string_view &coltype, kaitai::kstream *ks);
    bool is_multi_cell(const std::string_view &coltype);
    std::string_view get_child_type(const std::string_view &type);
    void get_map_child_types(const std::string_view &type, std::string_view *key_type, std::string_view *value_type);

    bool is_reversed(const std::string_view &type);
    bool is_composite(const std::string_view &type);
    bool is_list(const std::string_view &type);
    bool is_map(const std::string_view &type);
    bool is_set(const std::string_view &type);
    bool is_tuple(const std::string_view &type);

    std::shared_ptr<arrow::DataType> get_arrow_type(const std::string_view &type);
    arrow::Result<std::shared_ptr<struct node>> parse_nested_type(const std::string_view &type);

} // namespace conversions

#endif
