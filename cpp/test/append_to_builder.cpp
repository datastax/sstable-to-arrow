#define BOOST_TEST_MODULE Opaque Types
#include "sstable_to_arrow.h"
#include <boost/test/included/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_append_struct)
{
    const auto &complex_type = arrow::struct_(
        {arrow::field("list_of_uuids", arrow::list(arrow::fixed_size_binary(16))),
         arrow::field("nested_struct", arrow::struct_({arrow::field("nested_uuid", arrow::fixed_size_binary(16)),
                                                       arrow::field("nested_nonuuid", arrow::large_utf8())}))});

    std::unique_ptr<arrow::ArrayBuilder> builder;
    arrow::MakeBuilder(arrow::default_memory_pool(), complex_type, &builder);
}
