#ifndef IO_H_
#define IO_H_

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include "opts.h"

#define FAIL_ON_STATUS(x, msg) \
    if ((x) < 0)               \
    {                          \
        perror((msg));         \
        exit(1);               \
    }

#define SIZE_TO_BE(value)                                            \
    ((sizeof(size_t) == 2) ? htobe16(value)                          \
                           : ((sizeof(size_t) == 4) ? htobe32(value) \
                                                    : htobe64(value)))

arrow::Status send_tables(const std::vector<std::shared_ptr<arrow::Table>> &tables);
arrow::Status send_table(std::shared_ptr<arrow::Table> table, int cli_sockfd);
arrow::Status write_parquet(const std::string &path, std::shared_ptr<arrow::Table> table, arrow::MemoryPool *pool = arrow::default_memory_pool());

#endif
