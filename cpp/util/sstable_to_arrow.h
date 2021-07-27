#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <iostream>
#include <boost/algorithm/hex.hpp>
#include <arrow/api.h>
#include <thread>
#include <future>
#include <chrono>
#include <string_view>

#include "clustering_blocks.h"
#include "sstable_data.h"
#include "sstable_statistics.h"
#include "conversion_helper.h"
#include "opts.h"

// Convert the SSTable specified by `statistics` and `sstable` into an Arrow
// table, which is stored in `table`.
arrow::Status vector_to_columnar_table(
    std::shared_ptr<sstable_statistics_t> statistics,
    std::shared_ptr<sstable_data_t> sstable,
    std::shared_ptr<arrow::Table> *table,
    arrow::MemoryPool *pool = arrow::default_memory_pool());

arrow::Status process_partition(
    const std::unique_ptr<sstable_data_t::partition_t> &partition,
    std::unique_ptr<conversion_helper_t> &helper,
    arrow::MemoryPool *pool);

// Add each cell within the row given by `unfiltered`
arrow::Status process_row(
    sstable_data_t::row_t *row,
    bool is_static,
    const std::unique_ptr<conversion_helper_t> &helper,
    arrow::MemoryPool *pool);

// delegates to either append_simple or append_complex
arrow::Status append_cell(
    kaitai::kstruct *cell,
    const std::unique_ptr<conversion_helper_t> &helper,
    std::shared_ptr<column_t> col,
    arrow::MemoryPool *pool);

// Takes a collection of values in a complex cell and appends them to the corresponding arrow builder.
arrow::Status append_complex(
    std::shared_ptr<column_t> col,
    const std::unique_ptr<conversion_helper_t> &helper,
    const sstable_data_t::complex_cell_t *cell,
    arrow::MemoryPool *pool);

// Adds the timestamp information in a cell as well as the value to the corresponding arrow builder.
arrow::Status append_simple(
    std::shared_ptr<column_t> col,
    const std::unique_ptr<conversion_helper_t> &helper,
    sstable_data_t::simple_cell_t *cell,
    arrow::MemoryPool *pool);


template <typename T>
arrow::Status initialize_ts_map_builder(const std::unique_ptr<arrow::ArrayBuilder> &from_ptr, arrow::MapBuilder **builder_ptr, T **item_ptr);

template <typename T>
arrow::Status initialize_ts_list_builder(const std::unique_ptr<arrow::ArrayBuilder> &from_ptr, arrow::ListBuilder **builder_ptr, T **item_ptr);

/**
 * @brief Appends a scalar value to an Arrow ArrayBuilder corresponding to a certain CQL type given by `coltype`.
 * 
 * @param coltype the CQL data type of the column
 * @param builder_ptr a pointer to the arrow ArrayBuilder
 * @param bytes a buffer containing the bytes from the SSTable
 */
arrow::Status append_scalar(
    std::string_view coltype,
    arrow::ArrayBuilder *builder_ptr,
    std::string_view bytes,
    arrow::MemoryPool *pool);

// appends the cell's timestamp or null if it doesn't exist to `builder`.
arrow::Status append_ts_if_exists(
    column_t::ts_builder_t *builder,
    const std::unique_ptr<conversion_helper_t> &helper,
    sstable_data_t::simple_cell_t *cell);
// appends the cell's local deletion time or null if it doesn't exist to `builder`.
arrow::Status append_local_del_time_if_exists(
    column_t::local_del_time_builder_t *builder,
    const std::unique_ptr<conversion_helper_t> &helper,
    sstable_data_t::simple_cell_t *cell);
// appends the cell's TTL or null if it doesn't exist to `builder`.
arrow::Status append_ttl_if_exists(
    column_t::ttl_builder_t *builder,
    const std::unique_ptr<conversion_helper_t> &helper,
    sstable_data_t::simple_cell_t *cell);

// handle tombstones
arrow::Status process_marker(sstable_data_t::range_tombstone_marker_t *marker);

// check if `row` has the column specified by an `idx` referring to the overall SSTable.
bool does_cell_exist(sstable_data_t::row_t *row, const uint64_t &idx);

#endif
