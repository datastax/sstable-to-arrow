package com.datastax.sstablearrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cndb.bulkimport.BulkImportFileUtils;
import com.datastax.cndb.metadata.backup.BulkImportTaskSpec;
import com.datastax.cndb.metadata.storage.SSTableCreation;
import com.datastax.cndb.metadata.storage.SSTableData;
import org.apache.arrow.vector.FieldVector;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.OutputHandler;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static com.datastax.cndb.bulkimport.BulkImportFileUtils.compressFiles;
import static com.datastax.cndb.bulkimport.BulkImporter.getTenantId;

/**
 * Writes SSTables to disk. Each Parquet file should correspond to a single SSTable.
 */
public class SSTableWriterUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableWriterUtils.class);

    // the descriptor with the intended final ULID
    private final Descriptor descriptor;
    private final TableMetadata tableMetadata;
    private final CQLSSTableWriter writer;

    public SSTableWriterUtils(TableMetadata tableMetadata, Path baseDir)
    {
        this.tableMetadata = tableMetadata;
        SSTableUniqueIdentifier identifier = DescriptorUtils.fromNextValue();
        Path dataDir = baseDir.resolve(identifier.toString()).resolve(tableMetadata.keyspace).resolve(tableMetadata.name);
        try
        {
            Files.createDirectories(dataDir);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        this.descriptor = new Descriptor(dataDir.toFile(), tableMetadata.keyspace, tableMetadata.name, identifier);

        String insertStatement = getInsertStatement(tableMetadata);

        LOGGER.info("Writing SSTable {} with statement {}", this.descriptor, insertStatement);

        this.writer = CQLSSTableWriter.builder()
                .inDirectory(this.descriptor.getDirectory().toFile())
                .forTable(tableMetadata.toCQLCreateStatement())
                .using(insertStatement)
                .build();
    }

    /**
     * Get a CQL INSERT statement for all of the columns in a table.
     *
     * @param metadata The schema of the Cassandra table to get the insert statement for.
     */
    public static String getInsertStatement(TableMetadata metadata)
    {
        // TODO handle uppercase and lowercase, write tests for this
        StringJoiner columns = new StringJoiner(", ", "(", ")");
        StringJoiner values = new StringJoiner(", ", "(", ")");
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(metadata.allColumnsInSelectOrder(), Spliterator.ORDERED), false)
                .forEach(column -> {
                    columns.add(column.name.toString());
                    values.add("?");
                });
        return "INSERT INTO \"" + metadata.keyspace + "\".\"" + metadata.name + "\" " + columns + " VALUES " + values;
    }

    public static void putFile(BulkImportTaskSpec taskSpec, String destinationKey, Path sourcePath)
    {
        LOGGER.info("uploading data file {} to bucket {} at key {}", sourcePath, taskSpec.getSingleTenant(), destinationKey);

        PutObjectRequest uploadFile = PutObjectRequest.builder()
                .bucket(taskSpec.getSingleTenant())
                .key(destinationKey)
                .build();
        BulkImportFileUtils.instance.getCndbClient().putObject(uploadFile, sourcePath);

        LOGGER.info("upload complete");
    }

    /**
     * Loads an SSTable at the given path and returns a list of SSTableData objects for that table.
     *
     * @return A list of SSTableData objects for the SSTable files in the given directory.
     * @throws ExecutionException if the SSTable files could not be loaded.
     * @throws InterruptedException if the SSTable files could not be loaded.
     */
    public List<SSTableData> getSSTableData() throws ExecutionException, InterruptedException
    {
        LOGGER.info("Loading SSTable at directory {}", descriptor);
        SSTableLoader loader = new SSTableLoader(
                descriptor.getDirectory().toFile(), new SSTableToArrow.TestClient(), new OutputHandler.SystemOutput(true, true));
        loader.stream().get();
        List<SSTableReader> readers = loader.streamedSSTables();

        LOGGER.info("Loaded {} SSTables", readers.size());

        // modify the sstabledata so that the filename is the sstable's ULID
        return readers.stream()
                .map(ssTableReader -> {
                    SSTableData data = SSTableData.create(ssTableReader, SSTableCreation.withoutId(OperationType.UNKNOWN));
                    data.fileName = descriptor.relativeFilenameFor(Component.DATA);
                    return data;
                })
                .collect(Collectors.toList());
    }

    /**
     * Writes the given data (see params) to an SSTable on disk. Note that the resulting SSTable's generation will be
     * whatever CQLSSTableWriter generates (1, 2, etc) instead of the ULID expected by CNDB.
     *
     * @param data A mapping from Cassandra column identifiers to Arrow field vectors.
     * The SSTable files will be written under nested directories of the keyspace and table name.
     *
     * @return a descriptor identifying the written SSTable
     */
    public void writeRows(Map<ColumnIdentifier, FieldVector> data) throws IOException
    {
        int size = data.values()
                .stream()
                .findFirst()
                .get()
                .getValueCount();

        LOGGER.debug("Writing {} rows to SSTable", size);

        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < size; i++)
        {
            row.clear();
            for (ColumnMetadata columnMetadata : tableMetadata.columns())
            {
                FieldVector vector = data.get(columnMetadata.name);
                Object obj = ArrowUtils.toJavaClass(vector, i);

                row.put(columnMetadata.name.toString(), obj);
            }
            writer.addRow(row);
        }
    }

    /**
     * @param taskSpec the task spec
     *
     * @return the descriptor for the remote SSTable
     */
    public String uploadSSTables(BulkImportTaskSpec taskSpec) throws IOException
    {
        LOGGER.debug("uploading SSTable {} to tenant {}", this.descriptor, taskSpec.getSingleTenant());

        String tenantId = getTenantId(taskSpec.getSingleTenant());
        // data/{keyspace}/{table}-{tableId}
        String tableBucketPath = String.format("data/%s/%s/%s-%s/", tenantId, tableMetadata.keyspace, tableMetadata.name, tableMetadata.id.toHexString());

        List<Path> filePaths = new ArrayList<>();

        // get path to Data.db file and zip all others
        Path dataPath;

        try (Stream<Path> files = Files.list(descriptor.getDirectory()))
        {
            dataPath = files.map(sstableFile ->
                    {
                        if (Descriptor.componentFromFilename(sstableFile.toString()).type.equals(Component.Type.DATA))
                        {
                            return sstableFile;
                        }
                        else
                        {
                            filePaths.add(sstableFile);
                            return null;
                        }
                    }).filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No data file found"));
        }

        // Create the new descriptor for remote storage
        if (taskSpec.isArchive())
        {
            Path archivePath = descriptor.getDirectory().resolve("Archive.zip");
            compressFiles(filePaths, archivePath, descriptor);
            String archiveKey = tableBucketPath + descriptor.filenamePart() + "-Archive.zip";
            putFile(taskSpec, archiveKey, archivePath);
        }
        else
        {
            //  upload each of the files
            for (Path filePath : filePaths)
            {
                Component component = Descriptor.componentFromFilename(filePath.getFileName().toString());
                String destinationKey = tableBucketPath + descriptor.filenamePart() + "-" + component.toString();
                putFile(taskSpec, destinationKey, filePath);
            }
        }

        //  upload Data.db file
        String dataKey = tableBucketPath + descriptor.pathFor(Component.DATA).getFileName();
        putFile(taskSpec, dataKey, dataPath);

        return dataKey;
    }

    public void close() throws IOException
    {
        writer.close();
    }
}
