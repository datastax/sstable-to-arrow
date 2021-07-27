#include "vint.h"

vint_t::vint_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    int8_t first_byte = ks->read_s1();
    if ((first_byte & 0x80) == 0) // positive
        val_ = first_byte;
    else
    {
        // get number of leading set bits
        int size = (uint8_t)first_byte == 0xff ? 8 : __builtin_clz(~first_byte) - 24; // -24 since C++ treats it as a 4-byte integer but we only want the number of leading zeros in the single byte
        val_ = first_byte & (0xff >> size);                                           // get the value of the remaining part of it

        for (int i = 0; i < size; i++)
        {
            char c = ks->read_u1();
            val_ <<= 8;
            val_ |= c & 0xff;
        }
    }
}

// TODO
// decode a Java BigInteger
uint64_t vint_t::parse_java(const char *bytes, size_t size)
{
    int rtn = *(int *)bytes;
    for (int i = 1; i < size; ++i)
    {
        rtn <<= 8;
        rtn |= bytes[i] & 0xff;
    }
    return rtn;
}

uint64_t vint_t::val()
{
    return val_;
}
