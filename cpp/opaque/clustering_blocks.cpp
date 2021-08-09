#include "clustering_blocks.h"

const int CLUSTERING = deserialization_helper_t::CLUSTERING;

/**
 * See https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L351
 */
clustering_blocks_t::clustering_blocks_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    int offset = 0;
    int size = deserialization_helper_t::get_n_cols(CLUSTERING);
    values_.resize(size);
    while (offset < size)
    {
        uint64_t header = vint_t(ks).val();
        int limit = std::min(size, offset + 32);
        while (offset < limit)
        {
            std::string cql_type = deserialization_helper_t::get_col_type(CLUSTERING, offset);

            if (is_null(header, offset))
                values_[offset] = nullptr; // this is probably unsafe but idk a better way
            else if (is_empty(header, offset))
                values_[offset] = '\0';
            else
                values_[offset] = ks->read_bytes(conversions::get_col_size(cql_type, _io()));
            offset++;
        }
    }
}

bool is_null(uint64_t header, int i)
{
    uint64_t mask = 1 << ((i * 2) + 1);
    return (header & mask) != 0;
}

bool is_empty(uint64_t header, int i)
{
    uint64_t mask = 1 << (i * 2);
    return (header & mask) != 0;
}

std::vector<std::string> *clustering_blocks_t::values()
{
    return &values_;
}
