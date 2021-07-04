#ifndef CLUSTERING_BLOCKS_H_
#define CLUSTERING_BLOCKS_H_

#include <kaitai/kaitaistruct.h>
#include <kaitai/kaitaistream.h>
#include <algorithm>
#include <vector>
#include <string>
#include "deserialization_helper.h"
#include "vint.h"

class clustering_blocks_t : public kaitai::kstruct
{
    std::vector<std::string> values_;

public:
    clustering_blocks_t(kaitai::kstream *ks);
    std::vector<std::string> *values();
};

bool is_null(long long header, int i);
bool is_empty(long long header, int i);

#endif