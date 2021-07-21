#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <iostream>

#include <arrow/api.h>
#include <thread>
#include <future>
#include <chrono>
#include <string_view>

#include "clustering_blocks.h"
#include "sstable_data.h"
#include "sstable_statistics.h"
#include "timer.h"

class column_t
{
public:
    std::string cassandra_type;
    std::shared_ptr<arrow::Field> field;
    std::unique_ptr<arrow::ArrayBuilder> builder;
    std::unique_ptr<arrow::ArrayBuilder> ts_builder;

    // this constructor infers the arrow::DataType from the Cassandra type
    column_t(
        const std::string &name_,
        const std::string &cassandra_type_,
        arrow::MemoryPool *pool,
        bool complex_ts_allowed = true)
        : column_t(name_, cassandra_type_, conversions::get_arrow_type(cassandra_type_), pool, complex_ts_allowed) {}

    column_t(
        const std::string &name_,
        const std::string &cassandra_type_,
        std::shared_ptr<arrow::DataType> type_,
        arrow::MemoryPool *pool,
        bool complex_ts_allowed);

    // allocate enough memory for nrows elements in both the value and timestamp
    // builders
    arrow::Status reserve(uint32_t nrows);
};

class conversion_helper_t
{
public:
    conversion_helper_t(std::shared_ptr<sstable_statistics_t> statistics, arrow::MemoryPool *pool);

    std::shared_ptr<column_t> partition_key;
    std::vector<std::shared_ptr<column_t>> clustering_cols;
    std::vector<std::shared_ptr<column_t>> static_cols;
    std::vector<std::shared_ptr<column_t>> regular_cols;

    // metadata from the Statistics.db file
    sstable_statistics_t::serialization_header_t *metadata;
    sstable_statistics_t::statistics_t *statistics;

    // get the actual timestamp based on the epoch, the minimum timestamp in
    // this SSTable, and the given delta
    uint64_t get_timestamp(uint64_t delta) const;
    size_t num_data_cols() const;
    size_t num_ts_cols() const;
    arrow::Status reserve();
    std::shared_ptr<arrow::Schema> schema() const;
    arrow::Result<std::shared_ptr<arrow::Table>> to_table() const;
};

// Convert the SSTable specified by `statistics` and `sstable` into an Arrow
// table, which is stored in `table`.
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Table> *table, arrow::MemoryPool *pool = arrow::default_memory_pool());

// Recursively allocate memory for `nrows` elements in `builder` and its child
// builders.
arrow::Status reserve_builder(arrow::ArrayBuilder *builder, const int64_t &nrows);

// Add each cell within the row given by `unfiltered`
arrow::Status process_row(
    sstable_data_t::row_t *row,
    bool is_static,
    const std::unique_ptr<conversion_helper_t> &helper,
    arrow::MemoryPool *pool);

arrow::Status append_cell(
    kaitai::kstruct *cell,
    const std::unique_ptr<conversion_helper_t> &helper,
    std::shared_ptr<column_t> col,
    arrow::MemoryPool *pool);

arrow::Status append_complex(
    std::shared_ptr<column_t> col,
    const std::unique_ptr<conversion_helper_t> &helper,
    const sstable_data_t::complex_cell_t *cell,
    arrow::MemoryPool *pool);

arrow::Status append_simple(
    std::shared_ptr<column_t> col,
    const std::unique_ptr<conversion_helper_t> &helper,
    sstable_data_t::simple_cell_t *cell,
    arrow::MemoryPool *pool);

arrow::Status append_scalar(
    std::string_view coltype,
    arrow::ArrayBuilder *builder_ptr,
    std::string_view bytes,
    arrow::MemoryPool *pool);

arrow::Status append_ts(
    arrow::TimestampBuilder *builder,
    const std::unique_ptr<conversion_helper_t> &helper,
    sstable_data_t::simple_cell_t *cell);

arrow::Status process_marker(sstable_data_t::range_tombstone_marker_t *marker);

sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics);

bool does_cell_exist(sstable_data_t::row_t *row, const uint64_t &idx);

#endif
