# **How we can get Astra and Cassandra a ticket into the RapidsAI GPU ecosystem**

*Alex Cai*

The big picture goal is to enable GPU-accelerated analytic queries (using the [RAPIDS](https://rapids.ai/index.html) ecosystem) on the Cassandra database.

Speed of machine learning tasks on a GPU compared to a CPU (from [RAPIDS](https://rapids.ai/about.html)):

![speed](https://rapids.ai/assets/images/rapids-end-to-end-performance-chart-oss-page-r4.svg)

The RAPIDS ecosystem is highly accessible, and most common machine learning and data science libraries for CPUs have an equivalent RAPIDS library that runs on the GPU:

![familiar python apis](assets/rapids-vs-cpu-1.png)

![rapids equivalents](assets/rapids-vs-cpu-2.png)

Note the use of [Apache Arrow](http://arrow.apache.org/) as the underlying memory format. It's based on columns rather than rows, allowing faster analytic queries, and also supports zero-copy reads for extremely fast data access.

It also includes an inter-process communication (IPC) mechanism used to transfer an Arrow record batch (i.e. a table) between processes. The IPC format is identical to the in-memory format, which eliminates any extra copying or de/serialization costs.

---

## How do we do it? (Approaches)

We can take a few various approaches (details to come):

1. Most basic approach: fetch the data using the Cassandra driver, convert it into pandas, and then turn it into a cuDF.
2. Same as above, but skip the pandas step and transform data from the C* driver directly into an Arrow table.
3. Server side: read SSTables from disk using Cassandra code (OSS or DSE), serialize it using the Arrow IPC stream format and send it to client.
4. Same as above, but use own parsing implementation (with Kaitai Struct) instead of using Cassandra code.
5. Same as above but with GPU vectorization using [CUDA](https://developer.nvidia.com/cuda-zone) (TODO).
