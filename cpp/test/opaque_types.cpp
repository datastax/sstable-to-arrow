#define BOOST_TEST_MODULE Opaque Types
#include "vint.h"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/test/included/unit_test.hpp>
#include <iostream>
#include <kaitai/kaitaistream.h>
#include <memory>

BOOST_AUTO_TEST_CASE(variable_integers)
{
    auto ss = std::make_unique<std::stringstream>();
    auto ks = std::make_unique<kaitai::kstream>(ss.get());
    
    *ss << (char) 3;
    auto x = vint_t(ks.get());
    BOOST_TEST(x.val() == 3);

    // 101101101001
    *ss << (char) 0x8b << (char) 0x69;
    x = vint_t(ks.get());
    BOOST_TEST(x.val() == 2921);
}
