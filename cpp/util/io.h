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
#include "timer.h"

#define FAIL_ON_STATUS(x, msg) \
    if ((x) < 0)               \
    {                          \
        perror((msg));         \
        exit(1);               \
    }

arrow::Status send_data(std::shared_ptr<arrow::Table> table);
arrow::Status write_parquet(std::shared_ptr<arrow::Table> table, arrow::MemoryPool *pool = arrow::default_memory_pool());

#endif
