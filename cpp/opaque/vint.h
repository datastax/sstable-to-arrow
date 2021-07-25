#ifndef VINT_H_
#define VINT_H_

#include <kaitai/kaitaistruct.h>
#include <kaitai/kaitaistream.h>
#include <iostream>
#include <iomanip>
#include "timer.h"

class vint_t : public kaitai::kstruct
{
    uint64_t val_;

public:
    vint_t(kaitai::kstream *ks);
    static uint64_t parse_java(const char *bytes, size_t size);
    uint64_t val();
};

#endif
