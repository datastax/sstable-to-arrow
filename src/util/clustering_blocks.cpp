#include "clustering_blocks.h"

// List of Cassandra types with a fixed length
std::map<std::string, int> is_fixed_len{
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

/**
 * See https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L351
 */
clustering_blocks_t::clustering_blocks_t(kaitai::kstream *ks)
{
    int offset = 0;
    int size = deserialization_helper_t::get_n_clustering_cells();
    values_.resize(size);
    while (offset < size)
    {
        long long header = vint_t(ks).val();
        int limit = std::min(size, offset + 32);
        while (offset < limit)
        {
            std::string type_info = deserialization_helper_t::get_clustering_type(offset);
            if (is_null(header, offset))
                values_[offset] = nullptr; // this is probably unsafe but idk a better way
            else if (is_empty(header, offset))
                values_[offset] = ks->read_bytes(0);
            else if (is_fixed_len.count(type_info) != 0) // the type has variable length
                values_[offset] = ks->read_bytes(is_fixed_len[type_info]);
            else
            {
                long long len = vint_t(ks).val();
                if (len < 0)
                {
                    perror("error: corrupted file, read negative length");
                    exit(1);
                }
                // TODO handle maximum size
                values_[offset] = ks->read_bytes(len);
            }
            offset++;
        }
    }
}

bool is_null(long long header, int i)
{
    long long mask = 1 << ((i * 2) + 1);
    return (header & mask) != 0;
}

bool is_empty(long long header, int i)
{
    long long mask = 1 << (i * 2);
    return (header & mask) != 0;
}

std::vector<std::string> *clustering_blocks_t::values()
{
    return &values_;
}
