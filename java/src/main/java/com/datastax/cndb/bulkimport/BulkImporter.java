package com.datastax.cndb.bulkimport;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.InflaterInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cndb.metadata.backup.BulkImportTaskSpec;
import com.datastax.cndb.metadata.storage.SSTableData;
import com.datastax.sstablearrow.ArrowToSSTable;
import com.datastax.sstablearrow.DescriptorUtils;
import com.datastax.sstablearrow.ParquetReaderUtils;
import com.datastax.sstablearrow.SSTableWriterUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import static com.datastax.cndb.bulkimport.BulkImportUploadUtils.uploadSSTables;
import static com.datastax.cndb.bulkimport.BulkImporterHttpResource.OBJECT_MAPPER;

/**
 * Handles the BulkImport task. Downloads files from the tenant's remote storage and uploads SSTables to CNDB storage.
 */
public class BulkImporter
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImporter.class);

    public static String getTenantId(String datacenterId)
    {
        return datacenterId.substring(0, datacenterId.lastIndexOf('-'));
    }

    /**
     * Fetch the most recent schema file from a tenant's cloud storage bucket.
     *
     * @param datacenterId the tenant ID and bucket name, a UUID with a suffixed ordinal
     */
    public static JSONObject fetchSchemaFile(String datacenterId) throws IOException, ParseException
    {
        LOGGER.debug("fetching schema file for {}", datacenterId);

        String prefix = String.format("metadata_backup/%s/schema", getTenantId(datacenterId));
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(datacenterId)
                .prefix(prefix)
                .build();

        ListObjectsResponse res = BulkImportFileUtils.instance.getCndbClient().listObjects(listObjects);
        String schemaKey = "";
        int recentVersion = 0;
        for (S3Object obj : res.contents())
        {
            try
            {
                String[] filenameParts = obj.key()
                        .split("/");
                int version = Integer.parseInt(filenameParts[filenameParts.length - 1].split("_")[1]);
                if (version > recentVersion)
                {
                    recentVersion = version;
                    schemaKey = obj.key();
                }
            }
            catch (NumberFormatException | ArrayIndexOutOfBoundsException e)
            {
                LOGGER.warn("skipping invalid schema file {}", obj.key());
            }
        }

        if (schemaKey.isEmpty())
        {
            throw new FileNotFoundException("no schema file found");
        }

        LOGGER.debug("found schema file {}, downloading", schemaKey);

        GetObjectRequest getObject = GetObjectRequest.builder()
                .bucket(datacenterId)
                .key(schemaKey)
                .build();
        InputStream schemaFile = BulkImportFileUtils.instance.getCndbClient().getObjectAsBytes(getObject)
                .asInputStream();

        LOGGER.debug("download complete, parsing to JSON");

        InflaterInputStream stream = new InflaterInputStream(schemaFile);
        StringBuilder builder = new StringBuilder();
        byte[] buffer = new byte[512];
        int nread = -1;
        while ((nread = stream.read(buffer)) != -1)
        {
            builder.append(new String(Arrays.copyOf(buffer, nread)));
        }

        JSONParser parser = new JSONParser();
        return (JSONObject) parser.parse(builder.toString());
    }


    /**
     * Download a single parquet object from S3, split it into batches ("VectorSchemaRoot"s),
     * and create an SSTable for each batch.
     *
     * @param taskSpec the specification of the bulk import task
     * @param schemaFile the JSONObject containing the downloaded schema file
     */
    public static List<BulkImportTaskResult> handleParquetObject(BulkImportTaskSpec taskSpec, JSONObject schemaFile)
    {
        LOGGER.info("Handling parquet object {} in bucket {} for tenant {} and downloading to {}",
                taskSpec.getObjectKey(), taskSpec.getBucketName(), taskSpec.getSingleTenant(), BulkImportFileUtils.instance.getBaseDir());

        Pair<String, String> keyspaceAndTableName = ArrowToSSTable.getKeyspaceAndTableName(Paths.get(taskSpec.getObjectKey()));
        String tableId = keyspaceAndTableName.left + "." + keyspaceAndTableName.right;
        TableMetadata metadata = ArrowToSSTable.getSchema(schemaFile, keyspaceAndTableName.left, keyspaceAndTableName.right);
        if (metadata == null)
        {
            LOGGER.info("Could not find schema for table {}", tableId);
            return Collections.singletonList(BulkImportTaskResult.error(taskSpec.getObjectKey(), "Error getting Cassandra schema for table " + tableId));
        }

        // each Parquet file may result in multiple SSTables
        try
        {
            Path parquetPath = ArrowToSSTable.downloadFile(taskSpec, BulkImportFileUtils.instance.parquetDir());
            List<BulkImportTaskResult> results = new ArrayList<>();
            ParquetReaderUtils.read("file:" + parquetPath, root -> {
                try
                {
                    results.addAll(processVectorSchemaRoot(root, metadata, taskSpec, BulkImportFileUtils.instance.sstableDir())
                            .stream()
                            .map(datum -> BulkImportTaskResult.success(parquetPath.toString(), datum))
                            .collect(Collectors.toList()));
                }
                catch (Exception e)
                {
                    results.add(BulkImportTaskResult.error(taskSpec.getObjectKey(), "Error processing vector schema root: " + e.getMessage()));
                }
            });
            LOGGER.debug("Processed results: {}", OBJECT_MAPPER.writeValueAsString(results));
            return results;
        }
        catch (Exception e)
        {
            return Collections.singletonList(BulkImportTaskResult.error(taskSpec.getObjectKey(), "Error reading Parquet file: " + e.getMessage()));
        }
    }


    /**
     * Upload an in-memory Arrow Table (VectorSchemaRoot) to the given tenant bucket.
     *
     * @param root the VectorSchemaRoot to upload
     * @param cassandraSchema the Cassandra schema for the table
     * @param taskSpec the specification of the bulk import task
     * @param baseDir the base directory to store the SSTable files in
     *
     * @return SSTableData objects corresponding to the SSTable files created
     * @throws IOException if there is an error writing the SSTable files
     * @throws ExecutionException if there is an error writing the SSTable files
     * @throws InterruptedException if there is an error writing the SSTable files
     */
    public static List<SSTableData> processVectorSchemaRoot(VectorSchemaRoot root, TableMetadata cassandraSchema, BulkImportTaskSpec taskSpec, Path baseDir) throws IOException, ExecutionException, InterruptedException
    {
        LOGGER.debug("Processing vector schema root with {} rows for table {}.{} and tenant {}", root.getRowCount(), cassandraSchema.keyspace, cassandraSchema.name, taskSpec.getSingleTenant());

        Map<ColumnIdentifier, FieldVector> vectorMap = ArrowToSSTable.alignSchemas(root, cassandraSchema);
        Descriptor writtenSSTableDescriptor = SSTableWriterUtils.writeCqlSSTable(cassandraSchema, vectorMap, baseDir, true);
        List<SSTableData> ssTableData = SSTableWriterUtils.getSSTableData(writtenSSTableDescriptor.getDirectory());

        String dataComponentPath = uploadSSTables(taskSpec, writtenSSTableDescriptor.getDirectory(), cassandraSchema);
        for (SSTableData datum : ssTableData)
        {
            datum.fileName = Paths.get(dataComponentPath).getFileName().toString();
            datum.operationId = java.util.Optional.empty();
        }
        return ssTableData;
    }
}
