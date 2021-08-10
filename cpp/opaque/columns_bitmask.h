#ifndef COLUMN_BITMASK_H_
#define COLUMN_BITMASK_H_

#include <kaitai/kaitaistruct.h> // for kstruct
#include <stdint.h>              // for uint64_t
namespace kaitai
{
class kstream;
}

class columns_bitmask_t : public kaitai::kstruct
{
  public:
    uint64_t bitmask;
    columns_bitmask_t(kaitai::kstream *ks);
};

#endif
