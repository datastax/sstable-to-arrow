#ifndef CONVERSION_HELPER_H_
#define CONVERSION_HELPER_H_

#include "sstable_statistics.h"       // for sstable_statistics_t
#include <arrow/array/builder_base.h> // for ArrayBuilder
#include <arrow/result.h>             // for Result
#include <arrow/status.h>             // for Status
#include <arrow/type_fwd.h>           // for TimestampBuilder, field, Durat...
#include <memory>                     // for shared_ptr, unique_ptr
#include <stddef.h>                   // for size_t
#include <stdint.h>                   // for uint64_t, uint8_t, uint32_t
#include <string>                     // for string
#include <vector>                     // for vector
namespace arrow
{
class Array;
class DataType;
class Field;
class MemoryPool;
class Schema;
class Table;
} // namespace arrow

namespace sstable_to_arrow
{

class column_t
{
  public:
    using ts_builder_t = arrow::TimestampBuilder;
    using local_del_time_builder_t = arrow::TimestampBuilder;
    using ttl_builder_t = arrow::DurationBuilder;

    std::string cassandra_type;
    std::shared_ptr<arrow::Field> field;
    std::unique_ptr<arrow::ArrayBuilder> builder, ts_builder, local_del_time_builder, ttl_builder;

    // uuids require a second value column
    std::unique_ptr<arrow::ArrayBuilder> second;
    bool has_second;
    bool m_is_clustering;

    column_t(const std::string &name_, const std::string &cassandra_type_, std::shared_ptr<arrow::DataType> type_,
             bool is_clustering, bool needs_second)
        : cassandra_type(cassandra_type_),
          field(arrow::field(name_, type_)), m_is_clustering{is_clustering}, has_second{needs_second} {};

    /**
     * @brief create the data builder and time data builders for this column
     *
     * @param complex_ts_allowed Whether the type of the time data builders should
     * mirror the data builder if complex. For example, if this column stores lists
     * of data and complex_ts_allowed is set to true, each of the time data
     * builders will also store a list of data. Otherwise, they will store a single
     * timestamp for the entire list.
     * @return arrow::Status
     */
    arrow::Status init(arrow::MemoryPool *pool, bool complex_ts_allowed = true);

    // allocate enough memory for nrows elements in both the value and timestamp
    // builders
    arrow::Status reserve(uint32_t nrows);
    uint8_t append_to_schema(std::shared_ptr<arrow::Field> *schema) const;
    uint8_t append_to_schema(std::shared_ptr<arrow::Field> *schema, const std::string &ts_name) const;
    arrow::Result<uint8_t> finish(std::shared_ptr<arrow::Array> *ptr);
    arrow::Status append_null();

    bool has_metadata() const;
};

class conversion_helper_t
{
    size_t m_n_uuid_cols;

    /**
     * @brief Uses the metadata to call make_column for each column in the Cassandra table
     */
    arrow::Status create_columns();

    conversion_helper_t(const std::unique_ptr<sstable_statistics_t> &statistics);

  public:
    /**
     * @brief Create a new conversion helper object.
     */
    static arrow::Result<std::unique_ptr<conversion_helper_t>> create(
        const std::unique_ptr<sstable_statistics_t> &statistics);

    std::shared_ptr<column_t> partition_key; // also stores row liveness info in metadata
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

    arrow::Result<std::shared_ptr<column_t>> make_column(const std::string &name, const std::string &type,
                                                         bool is_clustering);

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

} // namespace sstable_to_arrow

#endif
