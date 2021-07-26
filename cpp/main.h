#ifndef MAIN_H_
#define MAIN_H_

#include <stdint.h>
#include <getopt.h>
#include <unistd.h>
#include <dirent.h>
#include <fstream>
#include <memory>
#include <boost/filesystem.hpp>
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
#include "io.h"
#include "opts.h"

struct sstable_t
{
    boost::filesystem::path statistics_path, data_path, index_path, summary_path, compression_info_path;
    std::shared_ptr<sstable_statistics_t> statistics;
    std::shared_ptr<sstable_data_t> data;
    std::shared_ptr<sstable_index_t> index;
    std::shared_ptr<sstable_summary_t> summary;
    std::shared_ptr<sstable_compression_info_t> compression_info;
};

arrow::Status process_sstable(std::shared_ptr<sstable_t> sstable);

void read_data(std::shared_ptr<struct sstable_t> sstable);
std::shared_ptr<sstable_data_t> read_decompressed_sstable(
    std::shared_ptr<sstable_compression_info_t> compression_info,
    const boost::filesystem::path &data_path);
template <typename T>
void read_sstable_file(const boost::filesystem::path &path, std::shared_ptr<T> *sstable_obj);
template <>
void read_sstable_file(const boost::filesystem::path &path, std::shared_ptr<sstable_statistics_t> *sstable_obj);

void debug_statistics(std::shared_ptr<sstable_statistics_t> statistics);
void debug_data(std::shared_ptr<sstable_data_t> data);
void debug_index(std::shared_ptr<sstable_index_t> index);
void debug_summary(std::shared_ptr<sstable_summary_t> summary);

void get_file_paths(const boost::filesystem::path &p, std::map<int, std::shared_ptr<sstable_t>> &sstables);
void open_stream(const boost::filesystem::path &path, std::ifstream *ifs);
void process_serialization_header(sstable_statistics_t::serialization_header_t *serialization_header);

#endif
