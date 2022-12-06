#ifndef CLUSTERING_BLOCKS_H_
#define CLUSTERING_BLOCKS_H_

#include <kaitai/kaitaistruct.h>
#include <stdint.h>
#include <string>
#include <vector>
namespace kaitai
{
class kstream;
}

class clustering_blocks_t : public kaitai::kstruct
{
    std::vector<std::string> values_;

  public:
    clustering_blocks_t(kaitai::kstream *ks);
    std::vector<std::string> *values();
    void _read();
};

bool is_null(uint64_t header, int i);
bool is_empty(uint64_t header, int i);

#endif
