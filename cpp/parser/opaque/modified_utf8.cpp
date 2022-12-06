#include "modified_utf8.h"
#include <kaitai/kaitaistream.h> // for kstream
#include <stdint.h>              // for uint8_t
#include <string>                // for string

// see
// https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#modified-utf-8
// TODO implement modified utf8 parser or find library
modified_utf8_t::modified_utf8_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    uint8_t c{0}, p{0xff};
    while (true)
    {
        c = ks->read_bytes(1)[0];
        if (c == 0x00 && p == 0x00)
        {
            break;
        }
        p = c;
    }
}

void modified_utf8_t::_read(){}