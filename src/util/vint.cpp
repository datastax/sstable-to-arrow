#include "vint.h"

vint_t::vint_t(kaitai::kstream *ks)
{
    char first_byte = ks->read_bytes(1)[0];
    if (first_byte >= 0)
    {
        val_ = first_byte;
        return;
    }

    // magic formulae from cassandra
    // -24 since we only want the last byte
    int size = __builtin_clz(~first_byte) - 24;
    val_ = first_byte & (0xff >> size);

    for (int i = 0; i < size; i++)
    {
        char b = ks->read_bytes(1)[0];
        val_ <<= 8;
        val_ |= b & 0xff; // don't think the "& 0xff" does anything if it's one byte, but oh well
    }
}

long long vint_t::val()
{
    return val_;
}
