package com.datastax.sstablearrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.datastax.cndb.metadata.storage.SSTableCreation;
import com.datastax.cndb.metadata.storage.SSTableData;
import com.datastax.cndb.sstable.ULIDBasedSSTableUniqueIdentifierFactory;
import org.apache.arrow.vector.FieldVector;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.OutputHandler;

public class SSTableWriterUtils
{
    /**
     * Get a CQL INSERT statement for all of the columns in a table.
     *
     * @param metadata The schema of the Cassandra table to get the insert statement for.
     */
    public static String getInsertStatement(TableMetadata metadata)
    {
        //        TODO handle uppercase and lowercase, write test
        StringBuilder sb = new StringBuilder("INSERT INTO \"" + metadata.keyspace + "\".\"" + metadata.name + "\" (");
        StringBuilder values = new StringBuilder(" VALUES (");
        boolean first = true;
        for (Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder(); it.hasNext(); )
        {
            ColumnMetadata col = it.next();
            if (!first)
            {
                sb.append(", ");
                values.append(", ");
            }
            sb.append(col.name);
            values.append("?");
            first = false;
        }
        sb.append(")");
        sb.append(values);
        sb.append(")");

        return sb.toString();
    }

    /**
     * Writes the given data (see params) to an SSTable on disk. Note that the resulting SSTable's generation will be
     * whatever CQLSSTableWriter generates (1, 2, etc) instead of the ULID expected by CNDB.
     *
     * @param metadata the schema of the Cassandra table
     * @param data a mapping from Cassandra column identifiers to Arrow field vectors
     * @param dataDir the directory for the SSTable files to be written to
     *
     * @return a list of SSTableData objects for passing to etcd
     */
    public static List<SSTableData> writeCqlSSTable(TableMetadata metadata, Map<ColumnIdentifier, FieldVector> data, Path dataDir, boolean useUlid) throws IOException, ExecutionException, InterruptedException
    {
        dataDir = dataDir.resolve(metadata.keyspace + File.separator + metadata.name);
        Files.createDirectories(dataDir);
        assert dataDir.toFile()
                .exists();
        System.out.println("writing SSTables to " + dataDir.toAbsolutePath());

        Descriptor desc = new Descriptor(dataDir.toFile(), metadata.keyspace, metadata.name, DescriptorUtils.fromNextValue());

        //        BigTableWriter.create(desc, )

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(desc.getDirectory()
                        .toFile())
                .forTable(metadata.toCQLCreateStatement())
                .using(getInsertStatement(metadata))
                .build();

        int size = data.values()
                .stream()
                .findFirst()
                .get()
                .getValueCount();

        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < size; i++)
        {
            row.clear();
            for (ColumnMetadata columnMetadata : metadata.columns())
            {
                FieldVector vector = data.get(columnMetadata.name);
                Object obj = ArrowUtils.toJavaClass(vector, i);

                //                ByteBufferUtil.UNSET_BYTE_BUFFER
                if (obj == null)
                {

                }

                row.put(columnMetadata.name.toString(), obj);
            }
            writer.addRow(row);
        }

        writer.close();

        //        if (useUlid) {
        //            for (File file : dataDir.toFile()
        //                    .listFiles()) {
        //                Files.move(file.toPath(), desc.pathFor(Descriptor.componentFromFilename(file)));
        //            }
        //        }

        SSTableLoader loader = new SSTableLoader(dataDir.toFile(), new SSTableToArrow.TestClient(), new OutputHandler.SystemOutput(true, true));
        loader.stream()
                .get();
        List<SSTableReader> readers = loader.streamedSSTables();

        return readers.stream()
                .map(ssTableReader -> SSTableData.create(ssTableReader, SSTableCreation.withoutId(OperationType.UNKNOWN)))
                .collect(Collectors.toList());
    }
}
