#ifndef OPTS_H_
#define OPTS_H_

#include <arrow/filesystem/s3fs.h> // for S3FileSystem
#include <iostream>                // for cout
#include <memory>                  // for shared_ptr

#define DEBUG_ONLY(msg)                                                                                                \
    do                                                                                                                 \
    {                                                                                                                  \
        if (global_flags.verbose)                                                                                      \
        {                                                                                                              \
            std::cout << (msg);                                                                                        \
        }                                                                                                              \
    } while (0)

struct flags
{
    bool include_metadata = true;
    bool for_cudf = false;
    bool is_s3 = false;
    bool verbose = false;

    std::shared_ptr<arrow::fs::S3FileSystem> s3fs;
};

extern flags global_flags;

#endif
