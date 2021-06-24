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

sstable_statistics_t *read_statistics(std::string);
sstable_data_t *read_data(std::string);
sstable_index_t *read_index(std::string);

#endif
