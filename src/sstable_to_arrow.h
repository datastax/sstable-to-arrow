#ifndef SSTABLE_TO_ARROW_H_
#define SSTABLE_TO_ARROW_H_

#include <arrow/api.h>
#include "sstable_data.h"

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
arrow::Status vector_to_columnar_table(std::shared_ptr<sstable_data_t> sstable, std::shared_ptr<arrow::Schema> *schema, std::shared_ptr<arrow::Table> *table);

#endif
