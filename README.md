# SSTable to Arrow

A project for parsing SSTables, used by the Apache Cassandra database, via the [Kaitai Struct](https://kaitai.io/) declarative language.

`src/` contains the source files to run the code.

- `src/ksy/` contains the Kaitai Struct declarations for the various SSTable classes.
- `src/util` contains different "opaque types" used by the Kaitai Struct classes.
- `main.cpp` is the main driver for this codebase.

`vis/` contains modified Kaitai Struct files for visualizing the various data formats.

- Remember that compiling to graphviz does not require the structure specification to actually mask the file on disk; it can be independently compiled regardless.
