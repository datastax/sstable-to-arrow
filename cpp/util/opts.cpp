#include "opts.h"

flags global_flags;

void read_options(int argc, char *const argv[])
{
    using namespace boost::filesystem;

    int opt;
    while ((opt = getopt(argc, argv, ":m:t:i:p:vdn")) != -1)
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
            break;
        case 'v':
            global_flags.verbose = true;
            break;
        case 'd':
            global_flags.detailed = true;
            break;
        case 'n': // turn off sending via network
            global_flags.listen = false;
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

    global_flags.read_sstable_dir = !(global_flags.summary_only || global_flags.statistics_only || global_flags.index_only);

    if (global_flags.read_sstable_dir)
    {
        if (optind == argc)
            global_flags.errors.push_back("must specify path to directory containing sstable files");
        else if (!is_directory(argv[optind]))
            global_flags.errors.push_back("the sstable directory path given is not a path to a directory");
        else
            global_flags.sstable_dir_path = argv[optind];
    }
}
