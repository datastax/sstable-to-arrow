#include "sstable_to_pyarrow.h"
#include "opts.h"
#include "sstable_to_arrow.h"
#include <arrow/api.h>
#include <arrow/python/api.h>
#include <arrow/python/pyarrow.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <iostream>
#include <vector>

struct sstable_conversion_exception
{
    std::string message;
};

char const *greet()
{
    return "Hello world!";
}

PyObject *read_sstables_wrapper(std::string path)
{
    std::map<int, std::shared_ptr<sstable_t>> sstables;
    if (boost::istarts_with(path, "s3://"))
    {
        s3_connection conn;
        auto result = get_file_paths_from_s3(path);
        if (!result.ok())
            throw sstable_conversion_exception{result.status().message()};
        sstables = result.ValueOrDie();
    }
    else
    {
        std::cout << "reading sstables from " << path << '\n';
        sstables = read_sstables(path);
    }

    auto result = convert_sstables(sstables);
    if (!result.ok())
        throw sstable_conversion_exception{result.status().message()};

    auto arrow_tables{result.ValueOrDie()};
    auto wrapped_tables{PyList_New(0)};
    std::for_each(arrow_tables.begin(), arrow_tables.end(),
                  [&wrapped_tables](auto table) { PyList_Append(wrapped_tables, arrow::py::wrap_table(table)); });
    return wrapped_tables;
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

void exception_translator(const sstable_conversion_exception &err)
{
    PyErr_SetString(PyExc_Exception, err.message.data());
}

// make sure the module name is the same as the name imported into python
// i.e. if you run "import hello" in python, the module name should be "hello" (and not "hello_ext")
BOOST_PYTHON_MODULE(MODULE_NAME)
{
    using namespace boost::python;
    register_exception_translator<sstable_conversion_exception>(exception_translator);
    def("greet", greet);
    def("create_table", create_table);
    def("read_sstables", read_sstables_wrapper);
}
