#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <iostream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "deserialization_helper.h"
#include "clustering_blocks.h"
#include "sstable_data.h"
#include "sstable_statistics.h"

struct cql_decimal_t
{
    int scale;
    long long val;
};

void append_builder(
    const std::shared_ptr<std::vector<std::string>> &types,
    const std::shared_ptr<std::vector<std::string>> &names,
    const std::shared_ptr<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> &arr,
    const std::string &cassandra_type,
    const std::string &name,
    arrow::MemoryPool *pool);

arrow::Status append_scalar(const std::string &coltype, arrow::ArrayBuilder *builder_ptr, const std::string &bytes, arrow::MemoryPool *pool);

std::shared_ptr<arrow::ArrayBuilder> create_builder(const std::string &type, arrow::MemoryPool *pool);
arrow::Status send_data(const std::shared_ptr<arrow::Schema> &schema, const std::shared_ptr<arrow::Table> &table);
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table);

std::shared_ptr<arrow::DataType> get_arrow_type(const std::string &type);

#endif
