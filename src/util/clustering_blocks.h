#ifndef CLUSTERING_BLOCKS_H_
#define CLUSTERING_BLOCKS_H_

#include <kaitai/kaitaistream.h>
#include <algorithm>
#include <vector>
#include <string>
#include <map>
#include "deserialization_helper.h"
#include "vint.h"

extern std::map<std::string, int> is_fixed_len;

class clustering_blocks_t
{
    std::vector<std::string> values_;

public:
    clustering_blocks_t(kaitai::kstream *ks);
    std::vector<std::string> *values();
};

bool is_null(long long header, int i);
bool is_empty(long long header, int i);

#endif
