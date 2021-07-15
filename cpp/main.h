#ifndef MAIN_H_
#define MAIN_H_

#include <stdint.h>
#include <getopt.h>
#include <unistd.h>
#include <dirent.h>
#include <fstream>
#include <memory>
#include <iostream>
#include <map>
#include <lz4.h>

#include "sstable_statistics.h"
#include "sstable_data.h"
#include "sstable_index.h"
#include "sstable_summary.h"
#include "sstable_compression_info.h"
#include "sstable_to_arrow.h"
#include "deserialization_helper.h"
#include "timer.h"

struct sstable_t
{
    std::string statistics_path;
    std::string data_path;
    std::string index_path;
    std::string summary_path;
    std::string compression_info_path;
    std::shared_ptr<sstable_statistics_t> statistics;
    std::shared_ptr<sstable_data_t> data;
    std::shared_ptr<sstable_index_t> index;
    std::shared_ptr<sstable_summary_t> summary;
    std::shared_ptr<sstable_compression_info_t> compression_info;
};

arrow::Status process_sstable(std::shared_ptr<struct sstable_t> sstable);

void read_data(std::shared_ptr<struct sstable_t> sstable);
template <typename T>
void read_sstable_file(const std::string &path, std::shared_ptr<T> *sstable_obj);
template <>
void read_sstable_file(const std::string &path, std::shared_ptr<sstable_statistics_t> *sstable_obj);

void debug_statistics(std::shared_ptr<sstable_statistics_t> statistics);
void debug_data(std::shared_ptr<sstable_data_t> data);
void debug_index(std::shared_ptr<sstable_index_t> index);
// void debug_summary(std::shared_ptr<sstable_summary_t> summary);

void read_options(int argc, char *argv[]);
bool ends_with(const std::string &s, const std::string &end);
void get_file_paths(const std::string &path, std::map<int, std::shared_ptr<struct sstable_t>> &sstables);
void open_stream(const std::string &path, std::ifstream *ifs);
void process_serialization_header(sstable_statistics_t::serialization_header_t *serialization_header);
std::shared_ptr<sstable_data_t> read_decompressed_sstable(std::shared_ptr<sstable_compression_info_t> compression_info, const std::string &data_path);

#endif
