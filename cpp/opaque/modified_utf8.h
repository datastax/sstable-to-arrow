#ifndef MODIFIED_UTF8_H_
#define MODIFIED_UTF8_H_

#include <iosfwd>
#include <kaitai/kaitaistruct.h>

class modified_utf8_t : public kaitai::kstruct
{
    std::stringstream ss;

  public:
    modified_utf8_t(kaitai::kstream *ks);
};

#endif
