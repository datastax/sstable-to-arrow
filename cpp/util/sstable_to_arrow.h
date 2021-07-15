#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <iostream>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <thread>
#include <future>
#include <chrono>
#include <string_view>
#include <parquet/arrow/writer.h>

#include "deserialization_helper.h"
#include "clustering_blocks.h"
#include "sstable_data.h"
#include "sstable_statistics.h"
#include "timer.h"

#define FAIL_ON_STATUS(x, msg) \
    if ((x) < 0)               \
    {                          \
        perror((msg));         \
        exit(1);               \
    }

arrow::Status send_data(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Table> table);
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table);
arrow::Status reserve_builder(arrow::ArrayBuilder *builder, const int64_t &nrows);
arrow::Status process_row(
    std::string_view partition_key,
    std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered,
    std::shared_ptr<std::vector<std::string>> types,
    std::shared_ptr<std::vector<std::unique_ptr<arrow::ArrayBuilder>>> arr,
    sstable_statistics_t::serialization_header_t *serialization_header,
    arrow::MemoryPool *pool = arrow::default_memory_pool());

arrow::Status process_column(
    std::shared_ptr<std::vector<std::string>> types,
    arrow::FieldVector &schema_vector,
    std::shared_ptr<std::vector<std::unique_ptr<arrow::ArrayBuilder>>> arr,
    const std::string &cassandra_type,
    const std::string &name,
    const std::shared_ptr<arrow::DataType> &data_type,
    int64_t nrows,
    arrow::MemoryPool *pool = arrow::default_memory_pool());

arrow::Status append_scalar(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const std::string_view &bytes, arrow::MemoryPool *pool);
arrow::Status append_complex(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool);

arrow::Status handle_cell(std::unique_ptr<kaitai::kstruct> cell_ptr, arrow::ArrayBuilder *builder_ptr, const std::string_view &coltype, bool is_multi_cell, arrow::MemoryPool *pool);

arrow::Status write_parquet(const arrow::Table &table, arrow::MemoryPool *pool);

sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics);

#endif
