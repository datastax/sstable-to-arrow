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

struct conversion_helper_t
{
    std::vector<std::string> types;                             // cql types of all columns
    arrow::FieldVector schema_vector;                           // name and arrow datatype of all columns
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders; // arrow builder for each column
    sstable_statistics_t::serialization_header_t *metadata;
    sstable_statistics_t::statistics_t *statistics;
};

// Convert the SSTable specified by `statistics` and `sstable` into an Arrow
// table, which is stored in `table`.
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Table> *table, arrow::MemoryPool *pool = arrow::default_memory_pool());

// Initialize `conversion_helper` and the builders for all of the columns in
// this table using the schema provided in the statistics file.
arrow::Status initialize_schema(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<arrow::Schema> *schema, std::unique_ptr<conversion_helper_t> *conversion_helper, arrow::MemoryPool *pool);

arrow::Status process_column(
    const std::unique_ptr<conversion_helper_t> &helper,
    const std::string &cassandra_type,
    const std::string &name,
    const std::shared_ptr<arrow::DataType> &data_type,
    arrow::MemoryPool *pool);

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
