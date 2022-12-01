#pragma once

// Hack
#define private public
#include "sstable_data.h"
#undef private


class streaming_sstable_data_t : public sstable_data_t::sstable_data_t{

public:
    streaming_sstable_data_t(kaitai::kstream* p__io, 
      kaitai::kstruct* p__parent = nullptr, 
      sstable_data_t* p__root = nullptr, 
      int step = 1000000
    );


private:
    void _read_some();

public:
    //void read_some();
    void read_some(){
        _read_some();
    }


    void set_step(int step) {
        m_step = step;
    }

    int get_step() {
        return m_step;
    }

    int is_eof() {
        return m__io->is_eof();
    }

private:
    int m_step;

};