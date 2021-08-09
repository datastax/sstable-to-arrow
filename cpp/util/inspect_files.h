#ifndef INSPECT_FILES_H_
#define INSPECT_FILES_H_

// eventually replace this file with opaque types in Ruby so that people can inspect
// the files through kaitai visualizer

#include "sstable_data.h"
#include "sstable_statistics.h"
#include "sstable_index.h"
#include "sstable_summary.h"
#include "conversion_helper.h"

void debug_statistics(std::shared_ptr<sstable_statistics_t> statistics);
void debug_data(std::shared_ptr<sstable_data_t> data);
void debug_index(std::shared_ptr<sstable_index_t> index);
void debug_summary(std::shared_ptr<sstable_summary_t> summary);

#endif
