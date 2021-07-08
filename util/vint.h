#ifndef VINT_H_
#define VINT_H_

#include <kaitai/kaitaistruct.h>
#include <kaitai/kaitaistream.h>
#include <iostream>
#include <iomanip>
#include "timer.h"

class vint_t : public kaitai::kstruct
{
    long long val_;

public:
    vint_t(kaitai::kstream *ks);
    static long long parse_java(const char *bytes, size_t size);
    long long val();
};

#endif
