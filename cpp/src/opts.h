#ifndef OPTS_H_
#define OPTS_H_

#include <arrow/filesystem/s3fs.h> // for S3FileSystem
#include <stdint.h>                // for int64_t

#include <boost/filesystem/path.hpp> // for path
#include <iostream>                  // for cout
#include <memory>                    // for shared_ptr
#include <string>                    // for string
#include <vector>                    // for vector

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
    bool summary_only = false;    // m
    bool statistics_only = false; // t
    bool index_only = false;      // i
    bool write_parquet = false;   // p
    bool listen = true;           // d
    bool verbose = false;         // v
    bool include_metadata = true; // c
    bool for_cudf = false;        // x
    bool show_help = false;       // h
    bool use_sample_data = false; // s
    bool read_sstable_dir = true;
    bool is_s3 = false;

    std::shared_ptr<arrow::fs::S3FileSystem> s3fs;

    boost::filesystem::path sstable_dir_path;
    boost::filesystem::path summary_path;
    boost::filesystem::path statistics_path;
    boost::filesystem::path index_path;
    boost::filesystem::path parquet_dst_path;
    std::vector<std::string> errors;
};

void read_options(int argc, char *const argv[]);

extern flags global_flags;
extern const std::string help_msg;
extern const boost::filesystem::path sample_data_path;

struct timer
{
    std::string m_name;
    int64_t m_start;
    timer();
    ~timer();
};

#endif
