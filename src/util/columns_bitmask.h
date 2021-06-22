#ifndef COLUMN_BITMASK_H_
#define COLUMN_BITMASK_H_

#include <kaitai/kaitaistream.h>
#include "vint.h"
#include "deserialization_helper.h"

class columns_bitmask_t
{
public:
    columns_bitmask_t(kaitai::kstream *ks);
};

#endif
