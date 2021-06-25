#ifndef VINT_H_
#define VINT_H_

#include <kaitai/kaitaistruct.h>
#include <kaitai/kaitaistream.h>

class vint_t : public kaitai::kstruct
{
    long long val_;

public:
    vint_t(kaitai::kstream *ks);
    long long val();
};

#endif
