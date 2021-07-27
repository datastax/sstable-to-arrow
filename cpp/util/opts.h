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
    bool summary_only = false;    // m
    bool statistics_only = false; // t
    bool index_only = false;      // i
    bool write_parquet = false;   // p
    bool listen = true;           // d
    bool verbose = false;         // v
    bool include_metadata = true; // c
    bool for_cudf = false;        // g
    bool show_help = false;       // h
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

const std::string help_msg =
    "\n"
    "========================= sstable-to-arrow =========================\n"
    "\n"
    "This is a utility for parsing SSTables on disk and converting them\n"
    "into the Apache Arrow columnar memory format to allow for faster\n"
    "analytics using a GPU.\n"
    "\n"
    "Usage:\n"
    "      ./sstable-to-arrow -h\n"
    "      ./sstable-to-arrow -m <summary_file_path>\n"
    "      ./sstable-to-arrow -t <statistics_file_path>\n"
    "      ./sstable-to-arrow -i <index_file_path>\n"
    "      ./sstable-to-arrow [-p <parquet_dest_path>] [-dvcg] <sstable_dir_path>\n"
    "          (sstable_dir_path is the path to the directory containing all of\n"
    "          the sstable files)\n"
    "\n"
    "       -m <summary_file_path>    read the given summary file\n"
    "    -t <statistics_file_path>    read the given statistics file\n"
    "         -i <index_file_name>    read the given index file\n"
    "       -p <parquet_dest_path>    export the sstable to the specified\n"
    "                                 path as a parquet file\n"
    "                           -d    turn off listening on the network (dry run)\n"
    "                           -v    verbose output for debugging\n"
    "                           -c    don't include metadata; does not get\n"
    "                                 rid of duplicates (compact)\n"
    "                           -g    convert types that aren't supported by\n"
    "                                 cudf to hex strings\n"
    "                           -h    show this help message\n";

#endif
