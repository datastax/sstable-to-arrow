#include "columns_bitmask.h"

columns_bitmask_t::columns_bitmask_t(kaitai::kstream *ks)
{
    vint_t vint(ks);
    long long encoded = vint.val();

    int superset_count = deserialization_helper_t::get_n_cols(deserialization_helper_t::REGULAR);

    if (encoded == 0)
    {
        // return superset
    }
    // if superset.size() >= 64
    else if (superset_count >= 64)
    {
        int delta = encoded;
        int column_count = superset_count - delta;
        if (column_count < superset_count / 2)
        {
            for (int i = 0; i < column_count; i++)
            {
                vint_t v(ks);
                // add stuff
            }
        }
        else
        {
            int idx = 0;
            int skipped = 0;
            while (true)
            {
                int next_missing_index;
                if (skipped < delta)
                {
                    vint_t val(ks);
                    next_missing_index = val.val();
                }
                else
                    next_missing_index = superset_count;
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
