#ifndef OPTS_H_
#define OPTS_H_

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <getopt.h>
#include <memory>
#include <map>
#include <iostream>

struct flags
{
    bool summary_only = false;
    bool statistics_only = false;
    bool index_only = false;
    bool write_parquet = false;
    bool listen = true;
    bool verbose = false;
    bool detailed = false;
    bool read_sstable_dir = true;

    std::string sstable_dir_path;
    std::string summary_path;
    std::string statistics_path;
    std::string index_path;
    std::string parquet_dst_path;
    std::vector<std::string> errors;
};

extern flags global_flags;

void read_options(int argc, char *const argv[]);

#endif
