#include "cli_args.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <getopt.h>

cli_args read_options(int argc, char *const argv[])
{
    using namespace boost::filesystem;

    cli_args args;

    int opt;
    while ((opt = getopt(argc, argv, ":m:t:i:p:dvcxhs")) != -1)
    {
        switch (opt)
        {
        case 'm':
            args.summary_only = true;
            if (!is_regular_file(optarg))
            {
                args.errors.push_back("the summary file path given is not a path to a file");
                break;
            }
            args.summary_path = optarg;
            break;
        case 't':
            args.statistics_only = true;
            if (!is_regular_file(optarg))
            {
                args.errors.push_back("the statistics file path given is not a path to a file");
                break;
            }
            args.statistics_path = optarg;
            break;
        case 'i':
            args.index_only = true;
            if (!is_regular_file(optarg))
            {
                args.errors.push_back("the index file path given is not a path to an file");
                break;
            }
            args.index_path = optarg;
            break;
        case 'p':
            args.write_parquet = true;
            args.parquet_dst_path = optarg;
            args.listen = false;
            args.include_metadata = false;
            break;
        case 'd': // turn off sending via network
            args.listen = false;
            break;
        case 'v':
            args.verbose = true;
            break;
        case 'c':
            args.include_metadata = false;
            break;
        case 'x':
            args.for_cudf = true;
            break;
        case 'h':
            args.show_help = true;
            break;
        case 's':
            args.use_sample_data = true;
            if (!exists(sample_data_path))
            {
                args.errors.push_back("could not find sample data");
                break;
            }
            args.sstable_dir_path = sample_data_path;
            break;
        case ':':
            args.errors.push_back("missing argument for option " + std::to_string(optopt));
            break;
        case '?':
            args.errors.push_back("unrecognized option " + std::to_string(optopt));
            break;
        default:
            args.errors.push_back("this code should never be reached");
            break;
        }
    }

    args.read_sstable_dir = !(args.summary_only || args.statistics_only || args.index_only);

    if (args.read_sstable_dir && !args.use_sample_data)
    {
        if (optind == argc)
            args.show_help = true;
        else
        {
            args.is_s3 = boost::istarts_with(argv[optind], "S3://");
            if (!args.is_s3 && !is_directory(argv[optind]))
                args.errors.push_back("the sstable directory path given "
                                      "is not a path to a directory");
            else
                args.sstable_dir_path = argv[optind];
        }
    }

    return args;
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

const std::string sample_data_path{"/home/sample_data/baselines/iot-5b608090e03d11ebb4c1d335f841c590"};
