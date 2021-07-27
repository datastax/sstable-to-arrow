# SSTable to Arrow

[![forthebadge](https://forthebadge.com/images/badges/check-it-out.svg)](https://forthebadge.com)

[![forthebadge](https://forthebadge.com/images/badges/made-with-c-plus-plus.svg)](https://forthebadge.com)

A project for parsing SSTables, used by the Apache Cassandra database, via the [Kaitai Struct](https://kaitai.io/) declarative language.

![general layout of the Cassandra Data.db file](visualization/results/data.png)

## Description

The big picture goal is to allow GPU-accelerated analytic queries on the Cassandra database. This would enable clients to do more analysis using the data and open the path to future developments.

1. Currently, the only "easy" way to do this is to load the database using the Cassandra driver (most likely in Python), and then convert it to a `pandas` DataFrame for calculations. Then we can convert the `pd.DataFrame` to a `cudf.DataFrame` for GPU acceleration.

2. We can marginally optimize the above by converting the data from the driver into an Arrow Table instead of a pandas DataFrame. We can then turn the Table into a `cudf.DataFrame` with minimal overhead.

3. To avoid using the driver and putting load on the Cassandra database, our next approach uses the Cassandra server source code, either OSS or DSE, to read the SSTable files for the respective version. Then, we can use the [Apache Arrow Java API](http://arrow.apache.org/docs/java/index.html) to convert the partitions into a series of Arrow `RecordBatch`es (essentially tables). We can then write these batches using the Arrow streaming format to a network socket output stream. On the client side in Python, we can then read the data using `pyarrow` into a `pyarrow.Table`, which converts to a `cudf.DataFrame` as above.

4. At the current stage (this project), to prepare for future parallelization using `CUDA` and to avoid the overhead of using JVM and spinning up Cassandra, we decided to try and read the SSTable files with our own implementation using C++. To read the complex SSTable binary files, we decided to use the [Kaitai Struct](https://kaitai.io/) library to write declarative format specifications using `.ksy` (essentially `YAML`) files. This will greatly decrease the cost of maintaining different database formats in the future and help with migration issues as it is easier to document. It also has the benefit of working across languages, including being able to compile to graphviz for some cool visualizations (see above).

5. The next step will be to introduce GPU parallelization using CUDA while reading the SSTable files.

## Overview

- `cpp/` contains the source code for `sstable-to-arrow`.
- `java/` contains the code for reading SSTables using Cassandra and is no longer under development.
- `test/` contains CQL queries that generate some useful sample data as well as some helper scripts to profile the different approaches.
- `client/` contains sample Python clients to read the tables from the server
- `visualization/` contains modified Kaitai Struct files for visualizing the various data formats via exporting to `graphviz`. The visualizations are currently quite rudimentary due to limitations of kaitai exporting to graphviz. It doesn't support kaitai opaque types and also doesn't show conditional fields. The current `visualization/*_modified.dot` files are modified by hand, and the `visualization/*.ksy` files are not meant to parse actual files but rather just to generate boilerplate graphviz code for the diagrams.

## Getting started + limitations + upcoming features

See [`cpp/README.md`](cpp/README.md).
