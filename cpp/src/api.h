#ifndef SSTABLE_TO_ARROW_API_H_
#define SSTABLE_TO_ARROW_API_H_

#include <arrow/result.h> // for Result
#include <map>            // for map
#include <memory>         // for shared_ptr
#include <string_view>    // for string_view
#include <vector>         // for vector
namespace arrow { class Table; }

namespace sstable_to_arrow
{

class sstable_t; // forward decl

namespace
{
class s3_connection
{
    bool m_ok;

  public:
    s3_connection();
    ~s3_connection();
    bool ok() const;
};
} // namespace

/**
 * @brief Convert a map of sstable_t objects into a vector of arrow tables.
 *
 * @param sstables a map of sstables from their number in the filename to the sstable_t object containing the path
 * information for that sstable
 * @return arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>
 */
arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> convert_sstables(
    std::map<int, std::shared_ptr<sstable_t>> sstables);

/**
 * @brief Read a map of sstables from a given path.
 *
 * @param path either a path on the local filesystem or one to an s3 bucket.
 * @return std::map<int, std::shared_ptr<sstable_t>> sstable_t objects mapped by their generation number
 */
arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> read_sstables(std::string_view path);

} // namespace sstable_to_arrow

#endif
