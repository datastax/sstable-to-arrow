#pragma once

#include "sstable_data.h"


class streaming_sstable_data_t : public sstable_data_t::sstable_data_t{

public:
    streaming_sstable_data_t(kaitai::kstream* p__io, 
      kaitai::kstruct* p__parent = nullptr, 
      sstable_data_t* p__root = nullptr 
    );

public:

    int is_eof() {
        return m__io->is_eof();
    }

};