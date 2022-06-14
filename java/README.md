# sstable-to-arrow Java version

Make sure to mark the `resources` directory as the Resources Root. This allows the code to discover the `cassandra.yaml` file inside.

You will also need to create the following directory before running the code: `build/test/cassandra/commitlog`. This is the field `commitlog_directory` in `cassandra.yaml`.