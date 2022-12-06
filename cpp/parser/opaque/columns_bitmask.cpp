#include "columns_bitmask.h"
#include "deserialization_helper.h" // for deserialization_helper_t, deseri...
#include "vint.h"                   // for vint_t
#include <stddef.h>                 // for size_t
namespace kaitai { class kstream; }

// see Columns::deserializeSubset
// https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/Columns.java#L524
// From UnfilteredSerializer::deserializeRowBody
// https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L562
columns_bitmask_t::columns_bitmask_t(kaitai::kstream *ks) : kaitai::kstruct(ks)
{
    bitmask = vint_t(ks).val();

    size_t superset_count = deserialization_helper_t::get_n_cols(deserialization_helper_t::curkind);
    if (superset_count >= 64)
    {
        uint64_t delta = bitmask;
        uint64_t column_count = superset_count - delta;
        if (column_count < superset_count / 2) // encode the columns that are set
        {
            for (size_t i = 0; i < column_count; i++)
            {
                vint_t idx(ks);

                // add stuff
            }
        }
        else // more columns are set than absent, so `bitmask` stores the absent
             // columns
        {
            size_t idx = 0;
            size_t skipped = 0;
            while (true)
            {
                size_t next_missing_index = skipped < delta ? vint_t(ks).val() : superset_count;
                while (idx < next_missing_index)
                {
                    // do stuff
                    idx++;
                }
                if (idx == superset_count)
                {
                    break;
                }
                idx++;
                skipped++;
            }
        }
    }
    else
    {
        deserialization_helper_t::set_bitmask(bitmask);
    }
}

void columns_bitmask_t::_read()
{
}