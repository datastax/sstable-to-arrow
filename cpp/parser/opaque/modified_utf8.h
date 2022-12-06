#ifndef MODIFIED_UTF8_H_
#define MODIFIED_UTF8_H_

#include <iosfwd>                // for stringstream
#include <kaitai/kaitaistruct.h> // for kstruct
namespace kaitai
{
class kstream;
} // namespace kaitai

class modified_utf8_t : public kaitai::kstruct
{
    std::stringstream ss;

  public:
    modified_utf8_t(kaitai::kstream *ks);
    void _read();
};

#endif
