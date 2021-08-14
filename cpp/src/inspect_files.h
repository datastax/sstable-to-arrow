#ifndef INSPECT_FILES_H_
#define INSPECT_FILES_H_

// eventually replace this file with opaque types in Ruby so that people can
// inspect the files through kaitai visualizer

#include <memory> // for unique_ptr
class sstable_data_t;
class sstable_index_t;
class sstable_statistics_t;
class sstable_summary_t;

namespace sstable_to_arrow
{

void debug_statistics(const std::unique_ptr<sstable_statistics_t> &statistics);
void debug_data(const std::unique_ptr<sstable_data_t> &data);
void debug_index(const std::unique_ptr<sstable_index_t> &index);
void debug_summary(const std::unique_ptr<sstable_summary_t> &summary);

} // namespace sstable_to_arrow

#endif
