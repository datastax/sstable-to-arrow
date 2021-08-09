#include "opts.h"

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

    global_flags.read_sstable_dir = !(
        global_flags.summary_only ||
        global_flags.statistics_only ||
        global_flags.index_only);

    if (global_flags.read_sstable_dir && !global_flags.use_sample_data)
    {
        if (optind == argc)
            global_flags.errors.push_back("must specify path to directory containing sstable files");
        else
        {
            global_flags.is_s3 = boost::istarts_with(argv[optind], "S3://");
            if (!global_flags.is_s3 && !is_directory(argv[optind]))
                global_flags.errors.push_back("the sstable directory path given is not a path to a directory");
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
