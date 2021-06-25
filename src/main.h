#ifndef MAIN_H_
#define MAIN_H_

#include <fstream>
#include <cstdint>
#include <memory>
#include <iostream>
#include <getopt.h>
#include <unistd.h>

#include "sstable_statistics.h"
#include "sstable_data.h"
#include "sstable_index.h"
#include "sstable_to_arrow.h"
#include "deserialization_helper.h"

void read_statistics(std::string path, std::shared_ptr<sstable_statistics_t> *sstable_statistics);
void read_data(std::string path, std::shared_ptr<sstable_data_t> *sstable);
void read_index(std::string path, std::shared_ptr<sstable_index_t> *index);

#endif
