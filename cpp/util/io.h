#ifndef IO_H_
#define IO_H_

#include <endian.h>           // for htobe16, htobe32, htobe64
#include <parquet/platform.h> // for default_memory_pool, Status
#include <stdio.h>            // for perror
#include <stdlib.h>           // for exit

#include <memory> // for shared_ptr
#include <string> // for string
#include <vector> // for vector
namespace arrow
{
class MemoryPool;
class Table;
} // namespace arrow

#define FAIL_ON_STATUS(x, msg)                                                                                         \
    if ((x) < 0)                                                                                                       \
    {                                                                                                                  \
        perror((msg));                                                                                                 \
        exit(1);                                                                                                       \
    }

#define SIZE_TO_BE(value)                                                                                              \
    ((sizeof(size_t) == 2) ? htobe16(value) : ((sizeof(size_t) == 4) ? htobe32(value) : htobe64(value)))

arrow::Status send_tables(const std::vector<std::shared_ptr<arrow::Table>> &tables);
arrow::Status send_table(std::shared_ptr<arrow::Table> table, int cli_sockfd);
arrow::Status write_parquet(const std::string &path, std::vector<std::shared_ptr<arrow::Table>> tables,
                            arrow::MemoryPool *pool = arrow::default_memory_pool());

#endif
