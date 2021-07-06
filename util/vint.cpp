#include "vint.h"

vint_t::vint_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    char first_byte = ks->read_bytes(1)[0];
    if (first_byte >= 0)
    {
        val_ = first_byte;
        return;
    }

    int size = __builtin_clz(~first_byte) - 24; // -24 since C++ treats it as a 4-byte integer but we only want the number of leading zeros in the last byte
    val_ = first_byte & (0xff >> size);         // get the value of the remaining part of it

    for (int i = 0; i < size; i++)
    {
        val_ <<= 8;
        val_ |= ks->read_bytes(1)[0] & 0xff;
    }
}

// TODO
// decode a Java BigInteger
long long vint_t::parse_java(const char *bytes, size_t size)
{
    int rtn = *(int *)bytes;
    for (int i = 1; i < size; ++i)
    {
        rtn <<= 8;
        rtn |= bytes[i] & 0xff;
    }
    return rtn;
}

long long vint_t::val()
{
    return val_;
}
