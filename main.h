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

#include "sstable_statistics.h"
#include "sstable_data.h"
#include "sstable_index.h"
#include "sstable_summary.h"
#include "sstable_to_arrow.h"
#include "deserialization_helper.h"
#include "timer.h"

struct sstable_files_t
{
    std::string data;
    std::string statistics;
    std::string index;
    std::string summary;
};

void read_statistics(const std::string &path, std::shared_ptr<sstable_statistics_t> *statistics);
void read_data(const std::string &path, std::shared_ptr<sstable_data_t> *sstable);
void read_index(const std::string &path, std::shared_ptr<sstable_index_t> *index);
void read_summary(const std::string &path, std::shared_ptr<sstable_summary_t> *summary);

void debug_statistics(std::shared_ptr<sstable_statistics_t> statistics);
void debug_data(std::shared_ptr<sstable_data_t> data);
void debug_index(std::shared_ptr<sstable_index_t> index);
void debug_summary(std::shared_ptr<sstable_summary_t> summary);

bool ends_with(const std::string &s, const std::string &end);
void get_file_paths(const std::string &path, std::map<int, struct sstable_files_t> &sstables);
void open_stream(const std::string &path, std::ifstream *ifs);
sstable_statistics_t::serialization_header_t *get_serialization_header(std::shared_ptr<sstable_statistics_t> statistics);
void process_serialization_header(sstable_statistics_t::serialization_header_t *serialization_header);

#endif
