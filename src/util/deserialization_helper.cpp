#include "deserialization_helper.h"

#define CHECK_KIND(kind) assert((kind) >= 0 && (kind) < 3)

typedef std::vector<std::string> strvec;

// List of Cassandra types with a fixed length
const std::map<std::string, int> is_fixed_len{
    {"org.apache.cassandra.db.marshal.BooleanType", 1},
    {"org.apache.cassandra.db.marshal.ByteType", 1},
    {"org.apache.cassandra.db.marshal.DoubleType", 8},
    {"org.apache.cassandra.db.marshal.DateType", 8},
    {"org.apache.cassandra.db.marshal.EmptyType", 0},
    {"org.apache.cassandra.db.marshal.FloatType", 4},
    {"org.apache.cassandra.db.marshal.Int32Type", 4},
    {"org.apache.cassandra.db.marshal.LexicalUUIDType", 16},
    {"org.apache.cassandra.db.marshal.LongType", 8},
    {"org.apache.cassandra.db.marshal.ShortType", 2},
    {"org.apache.cassandra.db.marshal.TimestampType", 8},
    {"org.apache.cassandra.db.marshal.TimeUUIDType", 16},
    {"org.apache.cassandra.db.marshal.UUIDType", 16},
    // {"org.apache.cassandra.db.marshal.CounterColumnType", 8},
    // TODO wth does ReversedType do?
    // https://github.com/apache/cassandra/blob/7486302d3ae4eac334e6669d7d4038b48fa6cce5/src/java/org/apache/cassandra/db/marshal/ReversedType.java#L135
};

// complex types
const std::set<std::string> is_multi_cell{
    "org.apache.cassandra.db.marshal.ListType",
    "org.apache.cassandra.db.marshal.MapType",
    "org.apache.cassandra.db.marshal.SetType"};

const std::vector<std::shared_ptr<strvec>> deserialization_helper_t::colkinds = {
    std::make_shared<strvec>(),
    std::make_shared<strvec>(),
    std::make_shared<strvec>()};

// we don't actually want to read any bytes
deserialization_helper_t::deserialization_helper_t(kaitai::kstream *ks_) : ks(ks_) {}

/** Get the number of clustering, static, or regular columns */
int deserialization_helper_t::get_n_cols(int kind)
{
    CHECK_KIND(kind);
    return colkinds[kind]->size();
}
/** Set the number of clustering, static, or regular columns and allocate memory for them */
void deserialization_helper_t::set_n_cols(int kind, int n)
{
    CHECK_KIND(kind);
    colkinds[kind]->resize(n);
}

/** Get the data kind stored in this column */
std::string deserialization_helper_t::get_col_type(int kind, int i)
{
    CHECK_KIND(kind);
    return (*colkinds[kind])[i];
}
void deserialization_helper_t::set_col_type(int kind, int i, std::string val)
{
    CHECK_KIND(kind);
    (*colkinds[kind])[i] = val;
}

int deserialization_helper_t::get_n_clustering_cells(int block) { return std::min(get_n_cols(CLUSTERING) - block * 32, 32); }
int deserialization_helper_t::get_n_blocks() { return (get_n_cols(CLUSTERING) + 31) / 32; }

bool deserialization_helper_t::is_complex() { return is_multi_cell.count(get_col_type(curkind, idx)) != 0; }

int deserialization_helper_t::inc()
{
    idx++;
    return 0;
}
int deserialization_helper_t::set_clustering()
{
    idx = 0;
    curkind = CLUSTERING;
    return 0;
}
int deserialization_helper_t::set_static()
{
    idx = 0;
    curkind = STATIC;
    return 0;
}
int deserialization_helper_t::set_regular()
{
    idx = 0;
    curkind = REGULAR;
    return 0;
}
int deserialization_helper_t::get_n_cols()
{
    // TODO get columns bitmask into consideration
    return get_n_cols(curkind);
}

int deserialization_helper_t::get_col_size()
{
    std::string coltype = get_col_type(curkind, idx);
    if (is_fixed_len.count(coltype) > 0)
        return is_fixed_len.find(coltype)->second;
    return vint_t(ks).val();
}
