#ifndef VINT_H_
#define VINT_H_

#include <kaitai/kaitaistream.h>

class vint_t
{
    long long val_;

public:
    vint_t(kaitai::kstream *ks);
    long long val();
};

#endif
