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

void read_summary(const std::string &path, std::shared_ptr<sstable_summary_t> *summary);
void read_statistics(const std::string &path, std::shared_ptr<sstable_statistics_t> *statistics);
void read_data(const std::string &path, std::shared_ptr<sstable_data_t> *sstable);
void read_index(const std::string &path, std::shared_ptr<sstable_index_t> *index);
void open_stream(const std::string &path, std::ifstream *ifs);

#endif
