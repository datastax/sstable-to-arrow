# sstable-to-arrow client

The Python notebooks here contain demos I used during my presentation. Use
`4_with_cuda` if your machine has CUDA installed, otherwise use `3_no_cuda`.

The big picture goal is to enable GPU-accelerated analytic queries (using the
[RAPIDS](https://rapids.ai/index.html) ecosystem) on the Cassandra database.

Note the use of [Apache Arrow](http://arrow.apache.org/) as the underlying
memory format. It's based on columns rather than rows, allowing faster analytic
queries, and also supports zero-copy reads for extremely fast data access.

It also includes an inter-process communication (IPC) mechanism used to transfer
an Arrow record batch (i.e. a table) between processes. The IPC format is
identical to the in-memory format, which eliminates any extra copying or
de/serialization costs.

## Getting started

See [this blog post](https://www.datastax.com/blog/analyzing-cassandra-data-using-gpus-part-2)
for detailed instructions.

1. Download the `no_cuda.py` script: `curl -LO https://github.com/datastax/sstable-to-arrow/blob/main/client/no_cuda.py`
3. Create a new virtualenv inside this directory `python -m venv ./myvenv`
4. Activate the virtualenv with `source ./myvenv/bin/activate`
5. Install the requirements by running `pip install pandas pyarrow`
6. Launch the sstable-to-arrow server with Docker: `docker run --rm -it -p 9143:9143 --name sstable-to-arrow datastaxlabs/sstable-to-arrow -s`
7. Run `python no_cuda.py`

## Next steps

- improve support for different types and cases in current implementation
- build a native SSTable reader in the cuDF project, e.g. `cudf.from_sstable()`
- read DSE SSTable format

## Other applications

- parsing different SSTable versions across different programming languages could help in migration from Cassandra to Astra
- could also use Kaitai to parse and document CQL protocol

## Speed comparison

Speed of machine learning tasks on a GPU compared to a CPU (from [RAPIDS](https://rapids.ai/about.html)):

![speed](https://rapids.ai/assets/images/rapids-end-to-end-performance-chart-oss-page-r4.svg)

The RAPIDS ecosystem is highly accessible, and most common machine learning and
data science libraries for CPUs have an equivalent RAPIDS library that runs on
the GPU:

![familiar python apis](assets/rapids-vs-cpu-1.png)

![rapids equivalents](assets/rapids-vs-cpu-2.png)

