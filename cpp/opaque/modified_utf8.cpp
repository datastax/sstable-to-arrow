#include "modified_utf8.h"

// see https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#modified-utf-8
// TODO
modified_utf8_t::modified_utf8_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    uint8_t c, p = 0xff;
    while (true)
    {
        c = ks->read_bytes(1)[0];
        if (c == 0x00 && p == 0x00)
            break;
        p = c;
    }
}
