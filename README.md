# SSTable to Arrow

A project for parsing SSTables, used by the Apache Cassandra database, via the [Kaitai Struct](https://kaitai.io/) declarative language.

`src/` contains the source files to run the code.

- `src/ksy/` contains the Kaitai Struct declarations for the various SSTable classes.
- `src/util` contains different "opaque types" used by the Kaitai Struct classes.
- `main.cpp` is the main driver for this codebase.

`vis/` contains modified Kaitai Struct files for visualizing the various data formats.

- Remember that compiling to graphviz does not require the structure specification to actually fit the file on disk; it can be independently compiled regardless.

## Description

The big picture goal is to allow GPU-accelerated analytic queries natively (or as close to it as possible) on the Cassandra database. This would enable our clients to do more analysis using the data and open the path to future developments.

1. Currently, the only easy way to do this is to load the database using the C* driver (most likely in Python), and then convert it to a `pandas` DataFrame for calculations. Then we can convert the `pd.DataFrame` to a `cudf.DataFrame` for GPU acceleration.

2. To get past any abstractions in the driver, our next approach used the Cassandra server source code (both OSS and DSE) to read the SSTable files directly in Java. Then, we used the [Apache Arrow Java API](http://arrow.apache.org/docs/java/index.html) to convert the partitions into a series of Arrow `RecordBatch`es. We can then pass these record batches through a `VectorSchemaRoot` to write them in the Arrow streaming format to a network socket output stream. On the client side in Python, we can then read the data using `pyarrow` into a `pyarrow.Table`, which converts to a `cudf.DataFrame` with little overhead.

3. At the current stage, to prepare for future parallelization using `CUDA`, we decided to try and read the SSTable files using C++. This introduces the difficult step of reading complex binary files. To solve this, we decided to use the [Kaitai Struct](https://kaitai.io/) library to write declarative format specifications using `YAML` files. This will greatly decrease the cost of maintaining different database formats in the future as it is close to self-documenting. It also has the benefit of working across languages. This is currently in progress as I'm working to add more types and making sure the conversions from Cassandra to Arrow types goes well.

4. The next step will be to actually introduce parallelization using CUDA.

## Getting started

1. This project depends on [Kaitai Struct](`https://kaitai.io/#download`), the [Kaitai Struct C++/STL runtime library](https://github.com/kaitai-io/kaitai_struct_cpp_stl_runtime), and [Apache Arrow for C++](http://arrow.apache.org/docs/cpp/cmake.html). (Note: if you are manually building Arrow and using other Arrow features like the filesystem interface `arrow::fs`, make sure to check [if you need to include any optional components](http://arrow.apache.org/docs/developers/cpp/building.html#optional-components).)

2. Get an SSTable. If you don't have one on hand, you can create one using CQL and the Cassandra Docker image. See [the quickstart](https://cassandra.apache.org/quickstart/) for more info.

```bash
docker network create cassandra
docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra:3.11
# runs CQL query in ./data.cql
# you may need to wait for the server to start up before running this
docker run --rm --network cassandra -v "$(pwd)/data.cql:/scripts/data.cql" -e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 nuvo/docker-cqlsh
# to open a CQL shell on the container, run:
# docker run --rm -it --network cassandra nuvo/docker-cqlsh cqlsh cassandra 9042 --cqlversion='3.4.4'
# go into the container
docker exec -it cassandra /bin/bash
# then run `nodetool flush` to copy the table
# then run `find / -name *-Data.db -type f`
# it will print out a list of files
# find the keyspace you wrote to and copy the path
# then exit to your local machine and run (replacing the path with the path to your keyspace)
docker cp cassandra:/path/to/your/keyspace ./res
# clean up
docker kill cassandra
docker network rm cassandra
```

3. Compile with `make` and run.
