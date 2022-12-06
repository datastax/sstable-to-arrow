#include "streaming_sstable_data.h"

streaming_sstable_data_t::streaming_sstable_data_t(kaitai::kstream* p__io, kaitai::kstruct* p__parent, sstable_data_t* p__root) : sstable_data_t(p__io) {
    sstable_data_t(p__io, p__parent, p__root);
}
