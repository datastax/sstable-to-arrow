#ifndef CLI_ARGS_H_
#define CLI_ARGS_H_

#include <vector>
#include <string>

struct cli_args
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
    bool only_uncompress = false; // u
    bool read_sstable_dir = true;
    bool is_s3 = false;

    std::string sstable_dir_path, summary_path, statistics_path, index_path, parquet_dst_path;

    std::vector<std::string> errors;
};

cli_args read_options(int argc, char *const argv[]);

extern const std::string help_msg;
extern const std::string sample_data_path;

#endif
