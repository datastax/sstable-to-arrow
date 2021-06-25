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
        long long header = vint_t(ks).val();
        int limit = std::min(size, offset + 32);
        while (offset < limit)
        {
            std::string type_info = deserialization_helper_t::get_col_type(CLUSTERING, offset);
            if (is_null(header, offset))
                values_[offset] = nullptr; // this is probably unsafe but idk a better way
            else if (is_empty(header, offset))
                values_[offset] = ks->read_bytes(0);
            else if (is_fixed_len.count(type_info) != 0) // the type has variable length
            {
                auto it = is_fixed_len.find(type_info);
                if (it == is_fixed_len.end())
                {
                    std::string err = "Invalid type: " + type_info;
                    perror(err.c_str());
                    exit(1);
                }
                values_[offset] = ks->read_bytes(it->second);
            }
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
