# SSTable to Arrow

A project for parsing SSTables, used by the Apache Cassandra database, via the [Kaitai Struct](https://kaitai.io/) declarative language.

![general layout of the Cassandra Data.db file](visualization/results/data.png)

## Description

The big picture goal is to allow GPU-accelerated analytic queries on the Cassandra database. This would enable our clients to do more analysis using the data and open the path to future developments.

1. Currently, the only "easy" way to do this is to load the database using the Cassandra driver (most likely in Python), and then convert it to a `pandas` DataFrame for calculations. Then we can convert the `pd.DataFrame` to a `cudf.DataFrame` for GPU acceleration.

2. We can marginally optimize the above by converting the data from the driver into an Arrow Table instead of a pandas DataFrame. We can then turn the Table into a `cudf.DataFrame` with minimal overhead.

3. To avoid using the driver and putting load on the Cassandra database, our next approach uses the Cassandra server source code, either OSS or DSE, to read the SSTable files for the respective version. Then, we can use the [Apache Arrow Java API](http://arrow.apache.org/docs/java/index.html) to convert the partitions into a series of Arrow `RecordBatch`es (essentially tables). We can then write these batches using the Arrow streaming format to a network socket output stream. On the client side in Python, we can then read the data using `pyarrow` into a `pyarrow.Table`, which converts to a `cudf.DataFrame` as above.

4. At the current stage (this project), to prepare for future parallelization using `CUDA` and to avoid the overhead of using JVM and spinning up Cassandra, we decided to try and read the SSTable files with our own implementation using C++. To read the complex SSTable binary files, we decided to use the [Kaitai Struct](https://kaitai.io/) library to write declarative format specifications using `.ksy` (essentially `YAML`) files. This will greatly decrease the cost of maintaining different database formats in the future and help with migration issues as it is easier to document. It also has the benefit of working across languages, including being able to compile to graphviz for some cool visualizations (see above).

5. The next step will be to actually introduce GPU parallelization using CUDA while reading the SSTable files. Hopefully this will only take a few changes to the current codebase, adding optimizations where necessary.

## Overview

- `ksy/` contains the Kaitai Struct declarations for the various SSTable classes.
- `util/` contains different "opaque types" (types defined outside of kaitai) used by the Kaitai Struct classes, as well as classes to help parse and transform the data.
- `res/cql/` contains CQL queries that generate some useful sample data.
- `res/data/` contains a few actual SSTable files for convenience.
- `visualization/` contains modified Kaitai Struct files for visualizing the various data formats via exporting to `graphviz`.

## Getting started

This project can be run through a Docker container via
```bash
docker build -t sstable-to-arrow .
docker run --rm -p 9143:9143 -d --name sstable-to-arrow sstable-to-arrow <PATH_TO_SSTABLE_DIRECTORY>
```
If not using Docker, you can manually build the project as follows, though installation of dependencies may vary from machine to machine.

1. This project depends on [Kaitai Struct](`https://kaitai.io/#download`), the [Kaitai Struct C++/STL runtime library](https://github.com/kaitai-io/kaitai_struct_cpp_stl_runtime), and [Apache Arrow for C++](http://arrow.apache.org/docs/cpp/cmake.html). (Note: if you are manually building Arrow and using other Arrow features like the filesystem interface `arrow::fs`, make sure to check [if you need to include any optional components](http://arrow.apache.org/docs/developers/cpp/building.html#optional-components).)
    * This requires `-DARROW_COMPUTE=ON` (and `-DARROW_CUDA=ON` in the future).

2. Get an SSTable. If you don't have one on hand, you can create one using CQL and the Cassandra Docker image using the steps below. See [the quickstart](https://cassandra.apache.org/quickstart/) for more info.

```bash
docker network create cassandra
docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra:3.11
# run a CQL query to create the data. you may need to wait for the server to start up before running this
docker run --rm --network cassandra -v "<LOCAL_PATH_TO_CQL_FILE>:/scripts/data.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 nuvo/docker-cqlsh
# to open a CQL shell on the container, run:
# docker run --rm -it --network cassandra nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.4'
docker exec cassandra /opt/cassandra/bin/nodetool flush
docker cp cassandra:/var/lib/cassandra/data/<YOUR_KEYSPACE> ./res
# clean up
docker kill cassandra
docker network rm cassandra
```

3. Compile as follows:

```bash
mkdir build
cd build
cmake ..
make
```

4. Run:

```bash
./sstable_to_arrow <PATH_TO_SSTABLE_DIRECTORY>
```

## Visualizations

The visualizations are currently quite rudimentary due to limitations of kaitai exporting to graphviz. It doesn't support kaitai opaque types and also doesn't show conditional fields. The current `visualization/*_modified.dot` files are edited by hand, and the `visualization/*.ksy` files are not meant to parse actual files but rather just to generate boilerplate graphviz code for the diagrams.

## Sample launch.json for VS Code users

```json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "g++ - Build and debug active file",
            "type": "cppdbg",
            "request": "launch",
            "program": "${workspaceFolder}/sstable_to_arrow",
            "args": ["<INSERT SSTABLE FOLDER HERE>"],
            "stopAtEntry": false,
            "cwd": "${workspaceFolder}",
            "environment": [],
            "externalConsole": false,
            "MIMode": "gdb",
            "setupCommands": [
                {
                    "description": "Enable pretty-printing for gdb",
                    "text": "-enable-pretty-printing",
                    "ignoreFailures": true
                }
            ],
            "miDebuggerPath": "/usr/bin/gdb"
        }
    ]
}
```

## TODO (Caveats)

- implement types
    - dates, decimals, frozen types, nested collections
- make sure tombstone markers are implemented correctly
- build via cmake? to make things easier
- deduplication?
- timing / load testing
- collect statistics on speed of different approaches
- test with larger datasets
