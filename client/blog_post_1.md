# Getting Cassandra a ticket into the RapidsAI GPU ecosystem

As a data-oriented organization, it’s not only important to locate and manage data effectively, but also analyze it to extract important insights that can help us make important decisions. Cassandra is already great at the former, and as for the latter, we can speed up analytics even more by integrating it with the RAPIDS ecosystem.

First, a quick introduction. [RAPIDS](https://rapids.ai/) is a suite of open source libraries for doing analytics and data science end-to-end on a GPU. Data science, and particularly machine learning, uses a lot of parallel calculations, which makes it better suited to run on a GPU, which can “multitask” at a few orders of magnitude higher than current CPUs:

![speed](https://rapids.ai/assets/images/rapids-end-to-end-performance-chart-oss-page-r4.svg)

Once we get the data on the GPU in the form of a cuDF DataFrame (cuDF is essentially the RAPIDS equivalent of pandas), we can interact with it using an almost identical API to the Python libraries you might be familiar with, such as pandas, scikit-learn, and more:

![familiar python apis](assets/rapids-vs-cpu-1.png)

![rapids equivalents](assets/rapids-vs-cpu-2.png)

(From https://docs.rapids.ai/overview/RAPIDS%200.13%20Release%20Deck.pdf)

Note the use of [Apache Arrow](http://arrow.apache.org/) as the underlying memory format. Arrow is based on columns rather than rows, allowing faster analytic queries. It also comes with an inter-process communication (IPC) mechanism used to transfer an Arrow record batch (i.e. a table) between processes. The IPC format is identical to the in-memory format, which eliminates any extra copying or de/serialization costs and gets us some extremely fast data access.

So the benefits of running analytics on a GPU are clear. All you need is the proper hardware, and you can migrate existing data science code to run on the GPU simply by switching out the names of the libraries.

## So how do we get Cassandra data onto the GPU?

Over the past few weeks, I've been taking a look at five different approaches in order, listed in order of increasing complexity below.

1. Fetch the data using the Cassandra driver, convert it into pandas, and then turn it into a cuDF.
2. Same as the above, but skip the pandas step and transform data from the driver directly into an Arrow table.
3. Read SSTables from the disk using Cassandra server code, serialize it using the Arrow IPC stream format and send it to the client.
4. Same as above, but use own parsing implementation (with Kaitai Struct) instead of using Cassandra code.
5. Same as above, but integrate GPU vectorization using [CUDA](https://developer.nvidia.com/cuda-zone) while parsing the SSTables.

First, I'll give a brief overview of each of these approaches, and then go
through a comparison at the end and explain our next steps.

## 1. Fetch data using the Cassandra driver

This approach is pretty simple, since you can use existing libraries without
having to do too much hacking. We grab the data from the driver, setting
`session.row_factory` to our `pandas_factory` function to tell the driver how to
transform the incoming data into a `pandas.DataFrame`. Then, it’s a simple
matter to call the `cudf.DataFrame.from_pandas` function to load our data onto
the GPU, where we can then use the RAPIDS libraries to run GPU-accelerated
analytics.

```py
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import pandas as pd
import pyarrow as pa
import cudf

import config

# connect to the Cassandra server in the cloud and configure the session settings
cloud_config= {
        'secure_connect_bundle': '/home/ubuntu/secure-connect-clitest.zip'
}
auth_provider = PlainTextAuthProvider(config.username, config.password)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect('clitest')
session.default_fetch_size = 1e7

def pandas_factory(colnames, rows):
    """Read the data returned by the driver into a pandas DataFrame"""
    return pd.DataFrame(rows, columns=colnames)
session.row_factory = pandas_factory

# run the CQL query and get the data
result_set = session.execute("select * from clitest.chipotle_stores limit 100;")
df = result_set._current_rows # a pandas dataframe with the information
gpu_df = cudf.DataFrame.from_pandas(df) # transform it into memory on the GPU
```

## 2. Fetch data using the Cassandra driver directly into Arrow

This step is identical to the previous one, except we can switch out
`pandas_factory` with the following `arrow_factory`:

```py
def get_col(col):
    rtn = pa.array(col) # automatically detects the type of the array

    # for a full implementation, we'd need to fully check which arrow types need
    # to be manually casted for compatibility with cudf
    if pa.types.is_decimal(rtn.type):
        return rtn.cast('float32')
    return rtn

def arrow_factory(colnames, rows):
    # convert from the row format passed by
    # CQL into the column format of arrow
    cols = [get_col(col) for col in zip(*rows)]
    table = pa.table({ colnames[i]: cols[i] for i in range(len(colnames)) })
    return table

session.row_factory = arrow_factory
```

We can then fetch the data and create the cudf in the same way.

However, both of these two approaches have a major drawback, which is that they
require the Cassandra server to be running, and query it for all of the data,
which is potentially computationally expensive and takes time we'd prefer to
leave for other operations.

Instead, we want to see if there's a way to get the data directly from the
SSTable files on the disk without going through the database. This brings us to
the next three approaches.

## 3. Read SSTables from the disk using Cassandra server code

Probably the easiest way to read SSTables on disk is to use the existing
Cassandra server technologies, namely [`SSTableLoader`](https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/io/sstable/SSTableLoader.java).
Once we have a list of partitions returned by `SSTableLoader.stream().get()`, we
can manually transform the data from Java objects into Arrow Vectors
corresponding to the *columns* of the table. Then, we can serialize the
collection of vectors (aka an Arrow RecordBatch, aka a `VectorSchemaRoot` in the
Java implementation) into the Arrow IPC stream format and then stream it in this
format across a socket.

The code here is more complex, and the transformation from CQL data types into
Arrow types is not always trivial.

Another drawback is that to use `SSTableLoader`, we first need to initialize the
Cassandra database, which takes a considerable amount of time on first load. It
also relies on the schema of the Cassandra table being stored on the actual
machine, since there's some information (like the names of the partition key
columns) that we can't actually deduce from the SSTable files.

## 4. Use a custom SSTable parser


