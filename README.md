# SSTable to Arrow

A project for parsing SSTables, used by the Apache Cassandra database, via the [Kaitai Struct](https://kaitai.io/) declarative language.

`src/` contains the source files to run the code.

- `src/ksy/` contains the Kaitai Struct declarations for the various SSTable classes.
- `src/util` contains different "opaque types" (types defined outside of kaitai) used by the Kaitai Struct classes.
- `main.cpp` is the main driver for this codebase.

`vis/` contains modified Kaitai Struct files for visualizing the various data formats.

- Remember that compiling to graphviz does not require the structure specification to actually fit the file on disk; it can be independently compiled regardless.

## Description

The big picture goal is to allow GPU-accelerated analytic queries natively (or as close to it as possible) on the Cassandra database. This would enable our clients to do more analysis using the data and open the path to future developments.

1. Currently, the only easy way to do this is to load the database using the Cassandra driver (most likely in Python), and then convert it to a `pandas` DataFrame for calculations. Then we can convert the `pd.DataFrame` to a `cudf.DataFrame` for GPU acceleration.

2. We can marginally optimize the above by converting the data from the driver into an Arrow Table instead of a pandas DataFrame. We can then turn the Table into a `cudf.DataFrame` with minimal (zero?) overhead.

3. To avoid using the driver and putting load on the Cassandra database, our next approach uses the Cassandra server source code (both OSS and DSE) to read the SSTable files. Then, we can use the [Apache Arrow Java API](http://arrow.apache.org/docs/java/index.html) to convert the partitions into a series of Arrow `RecordBatch`es (essentially tables). We can then write these batches using the Arrow streaming format to a network socket output stream. On the client side in Python, we can then read the data using `pyarrow` into a `pyarrow.Table`, which converts to a `cudf.DataFrame` as above.

4. At the current stage (this project), to prepare for future parallelization using `CUDA` and to avoid the overhead of using JVM and spinning up Cassandra, we decided to try and read the SSTable files with our own implementation using C++. To read the complex SSTable binary files, we decided to use the [Kaitai Struct](https://kaitai.io/) library to write declarative format specifications using `.ksy` (essentially `YAML`) files. This will greatly decrease the cost of maintaining different database formats in the future and help with migration issues as it is easier to document. It also has the benefit of working across languages, including being able to compile to graphviz for some cool visualizations:

    ![flowchart of Data.db file](visualization/results/data.png)

5. The next step will be to actually introduce GPU parallelization using CUDA. (TODO)

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
docker exec -t cassandra /opt/cassandra/bin/nodetool flush
# then run `nodetool flush` to copy the table
# then run `find / -name *-Data.db -type f`
# it will print out a list of files
# find the keyspace you wrote to and copy the path
# then exit to your local machine and run (replacing the path with the path to your keyspace)
docker cp cassandra:/var/lib/cassandra/data/<KEYSPACE> ./res
# clean up
docker kill cassandra
docker network rm cassandra
```

3. Compile with `make` and run.

## Visualizations

The visualizations are currently quite rudimentary due to limitations of kaitai exporting to graphviz. It doesn't support kaitai opaque types and also doesn't show conditional fields. The current `visualization/*_modified.dot` files are edited by hand, and the `visualization/*.ksy` files are not meant to parse actual files but rather just to generate boilerplate graphviz code for the diagrams.

## TODO

- test for different types
- make sure tombstone markers are implemented correctly
