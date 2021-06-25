#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <iostream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/filesystem/api.h>

#include "clustering_blocks.h"
#include "sstable_data.h"
#include "sstable_statistics.h"

#define EXIT_ON_FAILURE(expr)                            \
    do                                                   \
    {                                                    \
        arrow::Status status_ = (expr);                  \
        if (!status_.ok())                               \
        {                                                \
            std::cerr << status_.message() << std::endl; \
            return EXIT_FAILURE;                         \
        }                                                \
    } while (0);

arrow::Status send_data(const std::shared_ptr<arrow::Schema> &schema, const std::shared_ptr<arrow::Table> &table);
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_statistics_t> statistics, std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table);

#endif
