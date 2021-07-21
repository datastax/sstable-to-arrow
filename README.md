# SSTable to Arrow

A project for parsing SSTables, used by the Apache Cassandra database, via the [Kaitai Struct](https://kaitai.io/) declarative language.

![general layout of the Cassandra Data.db file](visualization/results/data.png)

## Description

The big picture goal is to allow GPU-accelerated analytic queries on the Cassandra database. This would enable clients to do more analysis using the data and open the path to future developments.

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

See [`cpp/README.md`](cpp/README.md).

## Visualizations

The visualizations are currently quite rudimentary due to limitations of kaitai exporting to graphviz. It doesn't support kaitai opaque types and also doesn't show conditional fields. The current `visualization/*_modified.dot` files are edited by hand, and the `visualization/*.ksy` files are not meant to parse actual files but rather just to generate boilerplate graphviz code for the diagrams.

## TODO (Caveats)

- sstable-to-arrow does not do deduping and sends each SSTable as an Arrow Table. The user must configure a cuDF per sstable and use the GPU to merge the sstables based on last write wins semantics. sstable-to-arrow exposes internal cassandra timestamps and tombstones so that merging can be done at the cuDF layer.
- Some information, including the names of the partition key and clustering columns, can't actually be deduced from the SSTable files and require the schema to be stored in the system tables.
- Cassandra stores data in memtables and commitlog before flushing to sstables, analytics performed via only sstable-to-arrow will potentially be stale / not real-time.
- Currently, the parser has only been tested with SSTables written by Cassandra OSS 3.11, which should be identical to SSTables written by Cassandra 3.x.
- The system is set up to scan entire sstables (not read specific partitions). More work will be needed if we ever do predicate pushdown.
- The following cql types are not supported: `counter`, `frozen`, and user-defined types.
- `varint`s can only store up to 8 bytes. Attempting to read a table with larger `varint`s will crash.
- The parser can only read tables with up to 64 columns.
- `decimal`s are converted into an 8-byte floating point value because neither C++ nor Arrow has native support for arbitrary-precision integers or decimals like of the Java `BigInteger` or `BigDecimal` classes. This means that operations on decimal columns will use floating point arithmetic, which may be inexact.
- `set`s are treated as lists since Arrow has no equivalent of a set.
