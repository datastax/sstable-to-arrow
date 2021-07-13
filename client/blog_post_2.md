# Getting Cassandra a ticket into the RapidsAI GPU ecosystem, Part 2

In the [previous post](blog_post_1), I talked briefly about the journey we've
taken to find the best way to get SSTable data loaded onto the GPU for data
analytics. We looked at the existing methods of pulling data from the driver and
manually converting it into a `cudf.DataFrame`, then at using the existing
Cassandra driver, and finally at working on a custom parser implementation,
which will be the focus of *this* post. I'll go over its capabilities,
limitations, and how to get started.

## Implementation details

The current parser is written in C++17. It uses the [Kaitai Struct](https://kaitai.io/)
library to declaratively specify the layout of the SSTable files using YAML. The
Kaitai Struct compiler then compiles these YAML files into C++ classes which can
then be included in the source code to actually parse the SSTables into data in
memory.

We then take this data and convert each column in the table into an `Arrow` vector.

## Limitations

### Versions

Currently, the parser only supports files written by Cassandra OSS 3.x. It also
doesn't support compressed files. The only files it actually loads are the
`Statistics.db` file for the table schema and the `Data.db` file for the actual
data.

### Types

The parser currently supports most [CQL types](https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cql_data_types_c.html)
except for `counter`, `frozen`, and user-defined types.

`varint`s can only store up to 8 bytes, and `decimal`s are converted into an
8-byte floating point value. This is because neither C++ nor Arrow has native
support for arbitrary-precision integers or decimals like of the Java
`BigInteger` or `BigDecimal` classes. Arrow *does* support arbitrary-length
binary values, but converting this to a numerical value on the client side isn't
exactly trivial.

`set`s are treated as lists since Arrow has no equivalent of a set.

## Roadmap and future developments

The ultimate goal for this project is to have some form of a `read_sstable`
function included in the RAPIDS ecosystem, similar to how the way `cudf.DataFrame.from_csv`
currently works.

Performance is going to be a continuous area of development, and I'm currently
looking into ways in which reading the SSTables can be further parallelized to
make the most use of the GPU.
