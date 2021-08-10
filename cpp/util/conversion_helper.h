#ifndef CONVERSION_HELPER_H_
#define CONVERSION_HELPER_H_

#include <arrow/array/builder_base.h> // for ArrayBuilder
#include <arrow/result.h>             // for Result
#include <arrow/status.h>             // for Status
#include <arrow/type_fwd.h>           // for TimestampBuilder, field, Durat...
#include <stddef.h>                   // for size_t
#include <stdint.h>                   // for uint64_t, uint8_t, uint32_t

#include <memory> // for shared_ptr, unique_ptr
#include <string> // for string
#include <vector> // for vector

#include "conversions.h" // for get_arrow_type
#include "opts.h"
#include "sstable_statistics.h" // for sstable_statistics_t
namespace arrow
{
class Array;
class DataType;
class Field;
class MemoryPool;
class Schema;
class Table;
} // namespace arrow

class column_t
{
  public:
    using ts_builder_t = arrow::TimestampBuilder;
    using local_del_time_builder_t = arrow::TimestampBuilder;
    using ttl_builder_t = arrow::DurationBuilder;

    std::string cassandra_type;
    std::shared_ptr<arrow::Field> field;
    std::unique_ptr<arrow::ArrayBuilder> builder;
    std::unique_ptr<arrow::ArrayBuilder> ts_builder;
    std::unique_ptr<arrow::ArrayBuilder> local_del_time_builder;
    std::unique_ptr<arrow::ArrayBuilder> ttl_builder;

    // uuids require a second value column
    std::unique_ptr<arrow::ArrayBuilder> second;
    bool has_second = false;

    // this constructor infers the arrow::DataType from the Cassandra type
    column_t(const std::string &name_, const std::string &cassandra_type_)
        : column_t(name_, cassandra_type_, conversions::get_arrow_type(cassandra_type_))
    {
    }

    column_t(const std::string &name_, const std::string &cassandra_type_, std::shared_ptr<arrow::DataType> type_)
        : cassandra_type(cassandra_type_),
          field(arrow::field(name_, type_)), has_second{conversions::is_uuid(cassandra_type_) &&
                                                        global_flags.for_cudf} {};

    arrow::Status init(arrow::MemoryPool *pool, bool complex_ts_allowed = true);

    // allocate enough memory for nrows elements in both the value and timestamp
    // builders
    arrow::Status reserve(uint32_t nrows);
    uint8_t append_to_schema(std::shared_ptr<arrow::Field> *schema) const;
    uint8_t append_to_schema(std::shared_ptr<arrow::Field> *schema, const std::string &ts_name) const;
    arrow::Result<uint8_t> finish(std::shared_ptr<arrow::Array> *ptr);
    arrow::Status append_null();
};

class conversion_helper_t
{
    size_t m_n_uuid_cols;

  public:
    conversion_helper_t(const std::unique_ptr<sstable_statistics_t> &statistics);

    std::shared_ptr<column_t> partition_key; // also stores row liveness info
    std::shared_ptr<arrow::TimestampBuilder> row_local_del_time;
    std::shared_ptr<arrow::TimestampBuilder> row_marked_for_deletion_at;
    std::shared_ptr<arrow::TimestampBuilder> partition_key_local_del_time;
    std::shared_ptr<arrow::TimestampBuilder> partition_key_marked_for_deletion_at;
    std::vector<std::shared_ptr<column_t>> clustering_cols;
    std::vector<std::shared_ptr<column_t>> static_cols;
    std::vector<std::shared_ptr<column_t>> regular_cols;

    // metadata from the Statistics.db file
    sstable_statistics_t::serialization_header_t *metadata;
    sstable_statistics_t::statistics_t *statistics;

    // get the actual timestamp based on the epoch, the minimum timestamp in
    // this SSTable, and the given delta
    arrow::Status init(arrow::MemoryPool *pool);

    uint64_t get_timestamp(uint64_t delta) const;
    uint64_t get_local_del_time(uint64_t delta) const;
    uint64_t get_ttl(uint64_t delta) const;
    arrow::Status append_partition_deletion_time(uint32_t local_deletion_time, uint64_t marked_for_delete_at);

    std::shared_ptr<column_t> make_column(const std::string &name, const std::string &type);

    size_t num_data_cols() const;
    size_t num_ts_cols() const;
    size_t num_cols() const;
    arrow::Status reserve();
    std::shared_ptr<arrow::Schema> schema() const;
    arrow::Result<std::shared_ptr<arrow::Table>> to_table() const;
};

// Recursively allocate memory for `nrows` elements in `builder` and its child
// builders.
arrow::Status reserve_builder(arrow::ArrayBuilder *builder, const int64_t &nrows);

// extract the serialization header from an SSTable
sstable_statistics_t::serialization_header_t *get_serialization_header(
    const std::unique_ptr<sstable_statistics_t> &statistics);

#endif
