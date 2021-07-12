#ifndef MODIFIED_UTF8_H_
#define MODIFIED_UTF8_H_

#include <kaitai/kaitaistruct.h>
#include <kaitai/kaitaistream.h>

class modified_utf8_t : public kaitai::kstruct
{
    std::stringstream ss;

public:
    modified_utf8_t(kaitai::kstream *ks);
};

#endif
