#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <iostream>

#include <arrow/api.h>
#include <thread>
#include <future>
#include <chrono>
#include <string_view>

#include "deserialization_helper.h"
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
    std::unique_ptr<arrow::TimestampBuilder> ts_builder;

    column_t(
        const std::string &name_,
        const std::string &cassandra_type_,
        arrow::MemoryPool *pool)
        : column_t(name_, cassandra_type_, conversions::get_arrow_type(cassandra_type_), pool) {}

    column_t(
        const std::string &name_,
        const std::string &cassandra_type_,
        std::shared_ptr<arrow::DataType> type_,
        arrow::MemoryPool *pool)
        : cassandra_type(cassandra_type_),
          field(arrow::field(name_, type_)),
          ts_builder(std::make_unique<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO), pool))
    {
        auto status = arrow::MakeBuilder(pool, field->type(), &builder);
        if (!status.ok())
        {
            std::cerr << "error making builder for column " << field->name() << '\n';
            exit(1);
        }
    }
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

    uint64_t get_timestamp(uint64_t delta);
    arrow::Status add_column(
        const std::string &cassandra_type,
        const std::string &name,
        const std::shared_ptr<arrow::DataType> &data_type,
        arrow::MemoryPool *pool);
    std::shared_ptr<arrow::Schema> schema();
    size_t num_cols();
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
    const std::unique_ptr<conversion_helper_t> &helper,
    arrow::MemoryPool *pool);

arrow::Status append_complex(
    std::string_view coltype,
    arrow::ArrayBuilder *builder_ptr,
    const sstable_data_t::complex_cell_t *cell,
    arrow::MemoryPool *pool);

arrow::Status append_scalar(
    std::string_view coltype,
    arrow::ArrayBuilder *builder_ptr,
    std::string_view bytes,
    arrow::MemoryPool *pool);

arrow::Status process_marker(sstable_data_t::range_tombstone_marker_t *marker);

sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics);

#endif
