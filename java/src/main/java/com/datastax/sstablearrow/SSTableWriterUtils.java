package com.datastax.sstablearrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cndb.metadata.storage.SSTableCreation;
import com.datastax.cndb.metadata.storage.SSTableData;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableWriterUtils.class);

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
     * @param metadata The schema of the Cassandra table.
     * @param data A mapping from Cassandra column identifiers to Arrow field vectors.
     * @param baseDir The base directory for the SSTable files.
     * The SSTable files will be written under nested directories of the keyspace and table name.
     *
     * @return a descriptor identifying the written SSTable
     */
    public static Descriptor writeCqlSSTable(TableMetadata metadata, Map<ColumnIdentifier, FieldVector> data, Path baseDir, boolean useUlid) throws IOException, ExecutionException, InterruptedException
    {
        Path dataDir = baseDir.resolve(metadata.keyspace).resolve(metadata.name);
        Files.createDirectories(dataDir);
        assert dataDir.toFile().exists();

        Descriptor desc = new Descriptor(dataDir.toFile(), metadata.keyspace, metadata.name, DescriptorUtils.fromNextValue());
        String insertStatement = getInsertStatement(metadata);
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(desc.getDirectory().toFile())
                .forTable(metadata.toCQLCreateStatement())
                .using(insertStatement)
                .build();

        int size = data.values()
                .stream()
                .findFirst()
                .get()
                .getValueCount();

        LOGGER.info("Writing SSTable {} with {} rows and with schema {}", desc, size, insertStatement);

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

        LOGGER.info("Wrote SSTable {}", desc);
        return desc;
    }

    /**
     * Loads an SSTable at the given path and returns a list of SSTableData objects for that table.
     *
     * @param dataDir The directory containing the SSTable files.
     *
     * @return A list of SSTableData objects for the SSTable files in the given directory.
     * @throws ExecutionException if the SSTable files could not be loaded.
     * @throws InterruptedException if the SSTable files could not be loaded.
     */
    public static List<SSTableData> getSSTableData(Path dataDir) throws ExecutionException, InterruptedException
    {
        LOGGER.info("Loading SSTable at directory {}", dataDir);
        SSTableLoader loader = new SSTableLoader(dataDir.toFile(), new SSTableToArrow.TestClient(), new OutputHandler.SystemOutput(true, true));
        loader.stream().get();
        List<SSTableReader> readers = loader.streamedSSTables();

        LOGGER.info("Loaded {} SSTables", readers.size());

        return readers.stream()
                .map(ssTableReader -> SSTableData.create(ssTableReader, SSTableCreation.withoutId(OperationType.UNKNOWN)))
                .collect(Collectors.toList());
    }
}
