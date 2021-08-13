#include <boost/python.hpp>
#include <arrow/api.h>
#include <arrow/python/api.h>
#include <arrow/python/pyarrow.h>
#include <iostream>

char const *greet()
{
    return "Hello world!";
}

// sample code for creating an arrow::Table
PyObject *create_table()
{
    // initialize pyarrow
    if (arrow::py::import_pyarrow() < 0)
    {
        std::cerr << "error importing pyarrow\n";
        return nullptr;
    }

    // create arrow::Array with test data
    arrow::Int32Builder builder;
    builder.Append(42);
    builder.Append(48);
    builder.Append(64);
    std::shared_ptr<arrow::Array> arr;
    arrow::Status st = builder.Finish(&arr);
    if (!st.ok())
    {
        std::cerr << "error finishing array\n";
        return nullptr;
    }

    // create an arrow::Table with a single column with the test data
    auto field1 = arrow::field("test", arrow::int32());
    auto schema = arrow::schema({field1});
    auto table = arrow::Table::Make(schema, {arr});

    // wrap the table into a PyObject
    return arrow::py::wrap_table(table);
}

// make sure the module name is the same as the name imported into python
// i.e. if you run "import hello" in python, the module name should be "hello" (and not "hello_ext")
BOOST_PYTHON_MODULE(hello)
{
    using namespace boost::python;
    def("greet", greet);
    def("create_table", create_table);
}
