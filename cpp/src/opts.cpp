#include "opts.h"

#include <bits/getopt_core.h> // for optarg, optind, getopt

#include <boost/algorithm/string/predicate.hpp> // for istarts_with
#include <boost/filesystem/operations.hpp>      // for is_regular_file, exists
#include <boost/filesystem/path_traits.hpp>     // for filesystem
#include <chrono>                               // for microseconds, time_p...
#include <iostream>                             // for operator<<, basic_os...
#include <type_traits>                          // for enable_if<>::type

flags global_flags;

void read_options(int argc, char *const argv[])
{
    using namespace boost::filesystem;

    int opt;
    while ((opt = getopt(argc, argv, ":m:t:i:p:dvcxhs")) != -1)
    {
        switch (opt)
        {
        case 'm':
            global_flags.summary_only = true;
            if (!is_regular_file(optarg))
            {
                global_flags.errors.push_back("the summary file path given is not a path to a file");
                break;
            }
            global_flags.summary_path = optarg;
            break;
        case 't':
            global_flags.statistics_only = true;
            if (!is_regular_file(optarg))
            {
                global_flags.errors.push_back("the statistics file path given is not a path to a file");
                break;
            }
            global_flags.statistics_path = optarg;
            break;
        case 'i':
            global_flags.index_only = true;
            if (!is_regular_file(optarg))
            {
                global_flags.errors.push_back("the index file path given is not a path to an file");
                break;
            }
            global_flags.index_path = optarg;
            break;
        case 'p':
            global_flags.write_parquet = true;
            global_flags.parquet_dst_path = optarg;
            global_flags.listen = false;
            global_flags.include_metadata = false;
            break;
        case 'd': // turn off sending via network
            global_flags.listen = false;
            break;
        case 'v':
            global_flags.verbose = true;
            break;
        case 'c':
            global_flags.include_metadata = false;
            break;
        case 'x':
            global_flags.for_cudf = true;
            break;
        case 'h':
            global_flags.show_help = true;
            break;
        case 's':
            global_flags.use_sample_data = true;
            if (!exists(sample_data_path))
            {
                global_flags.errors.push_back("could not find sample data");
                break;
            }
            global_flags.sstable_dir_path = sample_data_path;
            break;
        case ':':
            global_flags.errors.push_back("missing argument for option " + std::to_string(optopt));
            break;
        case '?':
            global_flags.errors.push_back("unrecognized option " + std::to_string(optopt));
            break;
        default:
            global_flags.errors.push_back("this code should never be reached");
            break;
        }
    }

    global_flags.read_sstable_dir =
        !(global_flags.summary_only || global_flags.statistics_only || global_flags.index_only);

    if (global_flags.read_sstable_dir && !global_flags.use_sample_data)
    {
        if (optind == argc)
            global_flags.show_help = true;
        else
        {
            global_flags.is_s3 = boost::istarts_with(argv[optind], "S3://");
            if (!global_flags.is_s3 && !is_directory(argv[optind]))
                global_flags.errors.push_back("the sstable directory path given "
                                              "is not a path to a directory");
            else
                global_flags.sstable_dir_path = argv[optind];
        }
    }
}

timer::timer()
{
    auto start_ts = std::chrono::high_resolution_clock::now();
    m_start = std::chrono::time_point_cast<std::chrono::microseconds>(start_ts).time_since_epoch().count();
}

timer::~timer()
{
    auto end_ts = std::chrono::high_resolution_clock::now();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_ts).time_since_epoch().count();
    std::cout << "[PROFILE " << m_name << "]: " << (end - m_start) << "us\n";
}

const std::string help_msg{"\n"
                           "========================= sstable-to-arrow =========================\n"
                           "\n"
                           "This is a utility for parsing SSTables on disk and converting them\n"
                           "into the Apache Arrow columnar memory format to allow for faster\n"
                           "analytics using a GPU.\n"
                           "\n"
                           "Usage:\n"
                           "      ./sstable-to-arrow -h\n"
                           "      ./sstable-to-arrow -m <summary_file_path>\n"
                           "      ./sstable-to-arrow -t <statistics_file_path>\n"
                           "      ./sstable-to-arrow -i <index_file_path>\n"
                           "      ./sstable-to-arrow [-p <parquet_dest_path>] [-dvcg] "
                           "<sstable_dir_path>\n"
                           "          (sstable_dir_path is the path to the directory containing "
                           "all "
                           "of\n"
                           "          the sstable files)\n"
                           "\n"
                           "       -m <summary_file_path>    read the given summary file\n"
                           "    -t <statistics_file_path>    read the given statistics file\n"
                           "         -i <index_file_name>    read the given index file\n"
                           "       -p <parquet_dest_path>    export the sstable to the specified\n"
                           "                                 path as a parquet file; implies -d "
                           "and "
                           "-c\n"
                           "                                 because certain metadata types are "
                           "not\n"
                           "                                 yet supported\n"
                           "                           -d    turn off listening on the network "
                           "(dry "
                           "run)\n"
                           "                           -v    verbose output for debugging\n"
                           "                           -c    don't include metadata; does not get\n"
                           "                                 rid of duplicates (compact)\n"
                           "                           -x    convert types that aren't supported "
                           "by\n"
                           "                                 cudf to hex strings\n"
                           "                           -s    read sample data; overwrites any "
                           "given "
                           "path\n"
                           "                           -h    show this help message\n"};

const boost::filesystem::path sample_data_path{"/home/sample_data/baselines/iot-5b608090e03d11ebb4c1d335f841c590"};
