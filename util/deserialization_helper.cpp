/**
 * Note that we use "kind" to refer to one of "clustering", "static", or "regular",
 * while we use "type" to refer to the actual data type stored inside a cell,
 * e.g. org.apache.cassandra.db.marshal.DateType.
 */

#include "deserialization_helper.h"
#include <iostream>

#define CHECK_KIND(kind) assert((kind) >= 0 && (kind) < 3)

typedef std::vector<std::string> strvec;

arrow::FieldVector decimal_fields{
    arrow::field("scale", arrow::int32()), arrow::field("val", arrow::binary())};

const std::map<std::string, struct cassandra_type> type_info{
    {"org.apache.cassandra.db.marshal.AsciiType", {"ascii", 0, arrow::utf8()}},        // ascii
    {"org.apache.cassandra.db.marshal.BooleanType", {"boolean", 1, arrow::boolean()}}, // boolean
    {"org.apache.cassandra.db.marshal.ByteType", {"tinyint", 1, arrow::int8()}},       // tinyint
    {"org.apache.cassandra.db.marshal.BytesType", {"blob", 0, arrow::binary()}},       // blob
    // {"org.apache.cassandra.db.marshal.CompositeType", { "", 0 }},
    // {"org.apache.cassandra.db.marshal.CounterColumnType", { "", 0 }}, // extends "long"
    // {"org.apache.cassandra.db.marshal.DateType", {"", 8}},             // old version of TimestampType
    {"org.apache.cassandra.db.marshal.DecimalType", {"decimal", 0, arrow::struct_(decimal_fields)}}, // decimal, custom implementation
    {"org.apache.cassandra.db.marshal.DoubleType", {"double", 8, arrow::float64()}},                 // double
    {"org.apache.cassandra.db.marshal.DurationType", {
                                                         "duration",
                                                         16,
                                                     }}, // duration
    // {"org.apache.cassandra.db.marshal.DynamicCompositeType", { "", 0 }},
    // {"org.apache.cassandra.db.marshal.EmptyType", { "", 0 }},
    {"org.apache.cassandra.db.marshal.FloatType", {"float", 4, arrow::float32()}}, // float
    // {"org.apache.cassandra.db.marshal.FrozenType", { "", 0 }},
    {"org.apache.cassandra.db.marshal.InetAddressType", {"inet", 0, arrow::int32()}}, // inet
    {"org.apache.cassandra.db.marshal.Int32Type", {"int", 4, arrow::int32()}},        // int
    {"org.apache.cassandra.db.marshal.IntegerType", {"varint", 0, arrow::binary()}},  // varint
    // {"org.apache.cassandra.db.marshal.LexicalUUIDType", { "", 16 }},
    // TODO ListType
    {"org.apache.cassandra.db.marshal.LongType", {"bigint", 8, arrow::int64()}}, // bigint
    // TODO MapType
    // {"org.apache.cassandra.db.marshal.PartitionerDefinedOrder", { "", 0 }}, // not for user-defined
    // https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/marshal/ReversedType.java
    // {"org.apache.cassandra.db.marshal.ReversedType", { "", 0 }}, // size of descendant
    // TODO SetType
    {"org.apache.cassandra.db.marshal.ShortType", {"smallint", 2, arrow::int16()}},                   // smallint
    {"org.apache.cassandra.db.marshal.SimpleDateType", {"date", 0, arrow::date32()}},                 // date, represented as 32-bit unsigned
    {"org.apache.cassandra.db.marshal.TimeType", {"time", 0, arrow::time64(arrow::TimeUnit::NANO)}},  // time
    {"org.apache.cassandra.db.marshal.TimeUUIDType", {"timeuuid", 16, arrow::fixed_size_binary(16)}}, // timeuuid
    {"org.apache.cassandra.db.marshal.TimestampType", {"timestamp", 8, arrow::uint64()}},             // timestamp
    // {"org.apache.cassandra.db.marshal.TupleType", { "", 0 }},
    {"org.apache.cassandra.db.marshal.UTF8Type", {"text", 0, arrow::utf8()}},                 // text, varchar
    {"org.apache.cassandra.db.marshal.UUIDType", {"uuid", 16, arrow::fixed_size_binary(16)}}, // uuid
    // {"org.apache.cassandra.db.marshal.UserType", { "", 0 }},
};

// complex types
const std::set<std::string> is_multi_cell{
    "org.apache.cassandra.db.marshal.ListType",
    "org.apache.cassandra.db.marshal.MapType",
    "org.apache.cassandra.db.marshal.SetType"};

// =============== DEFINE STATIC FIELDS ===============

int deserialization_helper_t::idx = 0;
int deserialization_helper_t::curkind = 0;

const std::vector<std::shared_ptr<strvec>> deserialization_helper_t::colkinds = {
    std::make_shared<strvec>(),
    std::make_shared<strvec>(),
    std::make_shared<strvec>()};

// =============== METHOD DECLARATIONS ===============

// we don't actually want to read any bytes from the file
deserialization_helper_t::deserialization_helper_t(kaitai::kstream *ks) : kaitai::kstruct(ks) {}

/** Get the number of clustering, static, or regular columns */
int deserialization_helper_t::get_n_cols(int kind)
{
    CHECK_KIND(kind);
    return colkinds[kind]->size();
}
/** Set the number of clustering, static, or regular columns (and allocate memory if setting) */
void deserialization_helper_t::set_n_cols(int kind, int n)
{
    CHECK_KIND(kind);
    colkinds[kind]->resize(n);
}

/** Get the data type stored in this column */
std::string deserialization_helper_t::get_col_type(int kind, int i)
{
    CHECK_KIND(kind);
    return (*colkinds[kind])[i];
}
/** Set the data type stored in this column */
void deserialization_helper_t::set_col_type(int kind, int i, const std::string &val)
{
    CHECK_KIND(kind);
    (*colkinds[kind])[i] = val;
}

/**
 * In each row, clustering cells are split up into blocks of 32
 * @returns how many columns are in the `block`-th clustering block
 */
int deserialization_helper_t::get_n_clustering_cells(int block)
{
    return std::min(get_n_cols(CLUSTERING) - block * 32, 32);
}
/**
 * Gets the number of clustering blocks total. Each one is a group of 32 clustering cells
 * Equivalent to (int)ceil((double)number_of_clustering_cells / 32.) but I avoid working with floats
 */
int deserialization_helper_t::get_n_blocks()
{
    return (get_n_cols(CLUSTERING) + 31) / 32;
}

/**
 * Checks if the currently selected cell is complex (usually a collection like a list, map, set, etc)
 */
bool deserialization_helper_t::is_complex(const std::string &coltype)
{
    int idx = coltype.find('(');
    return idx >= 0;
}
bool deserialization_helper_t::is_complex(int kind, int i)
{
    return is_complex(get_col_type(kind, i));
}
bool deserialization_helper_t::is_complex()
{
    return is_complex(curkind, idx);
}

/** =============== utility functions for the sstable_data.ksy file ===============
 * These are for when we need to evaluate certain portions imperatively due to
 * restrictions with Kaitai Struct
 */

// Increment the index of this helper (hover over the next cell)
int deserialization_helper_t::inc()
{
    idx++;
    return 0;
}
// Indicate that we are processing a static row
int deserialization_helper_t::set_static()
{
    std::cout << "begin processing static row\n";
    idx = 0;
    curkind = STATIC;
    return 0;
}
// Indicate that we are processing a regular row
int deserialization_helper_t::set_regular()
{
    std::cout << "begin processing regular row\n";
    idx = 0;
    curkind = REGULAR;
    return 0;
}
// get the number of columns stored in this sstable (aka the "superset" of columns)
// might not be the actual number of cells stored in this row
// see columns_bitmask.cpp
int deserialization_helper_t::get_n_cols()
{
    return get_n_cols(curkind);
}

int deserialization_helper_t::get_col_size(const std::string &coltype)
{
    std::cout << "getting col size of " << coltype << '\n';

    if (is_complex(coltype))
    {
        // it seems like children cells of a complex cell have their
        // size marked as a varint instead of the expected value...
        // std::string child_type(
        //     coltype.begin() + coltype.find('(') + 1,
        //     coltype.begin() + coltype.rfind(')'));
        // return get_col_size(child_type);
        long long len = vint_t(_io()).val();
        std::cout << "length of child cell: " << len << '\n';
        return len;
    }

    // check if this data type has a fixed length
    auto it = type_info.find(coltype);
    if (it == type_info.end())
    {
        perror(("unrecognized type: " + coltype).c_str());
        exit(1);
    }
    long long len;
    if (it->second.fixed_len != 0)
        len = it->second.fixed_len;
    // otherwise read the length as a varint
    else
        len = vint_t(_io()).val();
    std::cout << "length: " << len << '\n';
    return len;
}

// Get the size of the cell value in bytes
int deserialization_helper_t::get_col_size()
{
    return get_col_size(get_col_type(curkind, idx));
}
