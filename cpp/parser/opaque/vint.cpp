#include "vint.h"
#include <kaitai/kaitaistream.h>

vint_t::vint_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    uint8_t first_byte = ks->read_u1();
    if ((first_byte & 0x80) == 0) // positive
    {
        val_ = first_byte;
    }
    else
    {
        // get number of leading set bits
        size_t size = first_byte == 0xff ? 8
                                         : __builtin_clz(~first_byte & 0xff) -
                                               24; // -24 since C++ treats it as a 4-byte integer but we only
                                                   // want the number of leading zeros in the single byte

        // https://github.com/riptano/bdp/blob/d96027857317ddd79ab4d0044eb784e53e3a7919/dse-db/src/java/org/apache/cassandra/io/util/RebufferingInputStream.java#L222
        val_ = first_byte & (0xff >> size);        // get the value of the remaining part of it

        for (size_t i = 0; i < size; i++)
        {
            uint8_t c = ks->read_u1();
            val_ <<= 8;
            val_ |= c & 0xff;
        }
    }
}

// TODO decode a Java BigInteger
uint64_t vint_t::parse_java(const char *bytes, size_t size)
{
    uint64_t rtn = *reinterpret_cast<const int *>(bytes);
    for (size_t i = 1; i < size; ++i)
    {
        rtn <<= 8;
        rtn |= bytes[i] & 0xff;
    }
    return rtn;
}

uint64_t vint_t::val() const
{
    return val_;
}

void vint_t::_read()
{
}
