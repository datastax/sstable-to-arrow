#include "streaming_sstable_data.h"

streaming_sstable_data_t::streaming_sstable_data_t(kaitai::kstream* p__io, kaitai::kstruct* p__parent, sstable_data_t* p__root, int step) : sstable_data_t(p__io) {
    m_step = step;
    //sstable_data_t(p__io, p__parent, p__root);
}


void streaming_sstable_data_t::read_some() {
    m_deserialization_helper = std::unique_ptr<deserialization_helper_t>(new deserialization_helper_t(m__io));
    m_partitions = std::unique_ptr<std::vector<std::unique_ptr<partition_t>>>(new std::vector<std::unique_ptr<partition_t>>());
    {
        //int i = m_offset;
        int i = 0;
        //while (!m__io->is_eof()) {
        while (i < m_step && !m__io->is_eof()) {
            m_partitions->push_back(std::move(std::unique_ptr<partition_t>(new partition_t(m__io, this, m__root))));
            i++;
        }
    }
}