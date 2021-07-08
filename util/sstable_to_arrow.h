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

struct cql_decimal_t
{
    int scale;
    long long val;
};

arrow::Status send_data(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::Table> table);
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table);
arrow::Status process_row(
    std::string_view partition_key,
    std::unique_ptr<sstable_data_t::unfiltered_t> &unfiltered,
    std::shared_ptr<std::vector<std::string>> types,
    std::shared_ptr<std::vector<std::string>> names,
    std::shared_ptr<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> arr,
    arrow::MemoryPool *pool);

void process_column(
    std::shared_ptr<std::vector<std::string>> types,
    std::shared_ptr<std::vector<std::string>> names,
    std::shared_ptr<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> arr,
    const std::string &cassandra_type,
    const std::string &name,
    arrow::MemoryPool *pool);

std::shared_ptr<arrow::ArrayBuilder> create_builder(const std::string_view &type, arrow::MemoryPool *pool);

arrow::Status append_scalar(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const std::string_view &bytes, arrow::MemoryPool *pool);
arrow::Status append_scalar(const std::string_view &coltype, arrow::ArrayBuilder *builder_ptr, const sstable_data_t::complex_cell_t *cell, arrow::MemoryPool *pool);

#endif
