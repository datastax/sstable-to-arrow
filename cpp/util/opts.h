#ifndef OPTS_H_
#define OPTS_H_

#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <getopt.h>
#include <memory>
#include <map>
#include <iostream>

#define DEBUG_ONLY(msg)           \
    do                            \
    {                             \
        if (global_flags.verbose) \
            std::cout << (msg);   \
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

    boost::filesystem::path sstable_dir_path;
    boost::filesystem::path summary_path;
    boost::filesystem::path statistics_path;
    boost::filesystem::path index_path;
    boost::filesystem::path parquet_dst_path;
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
    "                                 path as a parquet file; implies -d and -c\n"
    "                                 because certain metadata types are not\n"
    "                                 yet supported\n"
    "                           -d    turn off listening on the network (dry run)\n"
    "                           -v    verbose output for debugging\n"
    "                           -c    don't include metadata; does not get\n"
    "                                 rid of duplicates (compact)\n"
    "                           -x    convert types that aren't supported by\n"
    "                                 cudf to hex strings\n"
    "                           -s    read sample data; overwrites any given path\n"
    "                           -h    show this help message\n";

const boost::filesystem::path sample_data_path{"/home/sample_data/baselines/iot-5b608090e03d11ebb4c1d335f841c590"};

#endif
