#ifndef MAIN_H_
#define MAIN_H_

#include "cli_args.h"
#include <arrow/api.h>
#include <map>
#include <memory>
#include <boost/filesystem.hpp>

class sstable_t;
struct cli_args;

/**
 * @brief If any specific files (index/statistics/summary) are passed for
 * inspection, inspect them using the functions in inspect_files
 */
arrow::Status run_arguments(cli_args args);

#endif
