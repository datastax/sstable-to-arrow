#include "columns_bitmask.h"

// see Columns::deserializeSubset https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/Columns.java#L524
// From UnfilteredSerializer::deserializeRowBody https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L562

columns_bitmask_t::columns_bitmask_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    long long encoded = vint_t(ks).val();

    // auto superset = deserialization_helper_t::get_vec_ptr();
    int superset_count = deserialization_helper_t::get_n_cols(deserialization_helper_t::REGULAR);

    if (encoded == 0)
    {
        // return superset
    }
    else if (superset_count >= 64)
    {
        int delta = encoded;
        int column_count = superset_count - delta;
        if (column_count < superset_count / 2) // encode the columns that are set
        {
            for (int i = 0; i < column_count; i++)
            {
                vint_t idx(ks);

                // add stuff
            }
        }
        else // more columns are set than absent, so `encoded` stores the absent columns
        {
            int idx = 0;
            int skipped = 0;
            while (true)
            {
                int next_missing_index = skipped < delta ? vint_t(ks).val() : superset_count;
                while (idx < next_missing_index)
                {
                    // do stuff
                    idx++;
                }
                if (idx == superset_count)
                    break;
                idx++;
                skipped++;
            }
        }
    }
    else
    {
        // get column info
    }
}
