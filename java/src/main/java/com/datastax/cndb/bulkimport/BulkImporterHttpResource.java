package com.datastax.cndb.bulkimport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.ws.rs.POST;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cndb.metadata.storage.SSTableData;
import com.datastax.sstablearrow.ArrowToSSTable;
import com.datastax.sstablearrow.DescriptorUtils;
import com.datastax.sstablearrow.ParquetReaderUtils;
import com.datastax.sstablearrow.SSTableWriterUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The HTTP service for bulk import. To be made part of CNDB.
 */
@javax.ws.rs.Path("/api/v0/bulkimport")
public class BulkImporterHttpResource
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImporterHttpResource.class);

    private static final S3Client s3 = S3Client.builder()
            .region(Region.US_WEST_2)
            .build();

    private static final S3Client s3Admin = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("bonus"))
            .region(Region.US_EAST_1)
            .build();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module());

    /**
     * Download a single parquet object from S3, split it into batches,.
     */
    public static List<BulkImportTaskResult> handleParquetObject(String bucket, S3Object object, JSONObject schemaFile, String tenantId, Path outdir, boolean archive)
    {
        LOGGER.info("Handling parquet object {} in bucket {} for tenant {} with schema file {} and downloading to {}", object.key(), bucket, tenantId, schemaFile, outdir);

        Pair<String, String> keyspaceAndTableName = ArrowToSSTable.getKeyspaceAndTableName(Paths.get(object.key()));
        String tableId = keyspaceAndTableName.left + "." + keyspaceAndTableName.right;
        TableMetadata metadata = ArrowToSSTable.getSchema(schemaFile, keyspaceAndTableName.left, keyspaceAndTableName.right);
        if (metadata == null)
        {
            LOGGER.info("Could not find schema for table {}", tableId);
            return Collections.singletonList(BulkImportTaskResult.error(object.key(), "Error getting Cassandra schema for table " + tableId));
        }

        // each Parquet file may result in multiple SSTables
        try
        {
            Path parquetPath = ArrowToSSTable.downloadFile(s3, bucket, object, outdir);
            List<BulkImportTaskResult> results = new ArrayList<>();
            ParquetReaderUtils.read("file:" + parquetPath, root -> {
                try
                {
                    results.addAll(processVectorSchemaRoot(root, metadata, tenantId, archive).stream()
                            .map(datum -> BulkImportTaskResult.success(parquetPath.toString(), datum))
                            .collect(Collectors.toList()));
                }
                catch (Exception e)
                {
                    results.add(BulkImportTaskResult.error(object.key(), "Error processing vector schema root: " + e.getMessage()));
                }
            });
            LOGGER.debug("Processed results: {}", OBJECT_MAPPER.writeValueAsString(results));
            return results;
        }
        catch (Exception e)
        {
            return Collections.singletonList(BulkImportTaskResult.error(object.key(), "Error reading Parquet file: " + e.getMessage()));
        }
    }

    public static List<SSTableData> processVectorSchemaRoot(VectorSchemaRoot root, TableMetadata cassandraSchema, String tenantId, boolean archive) throws IOException, ExecutionException, InterruptedException
    {
        LOGGER.debug("Processing vector schema root with {} rows for table {}.{} and tenant {}", root.getRowCount(), cassandraSchema.keyspace, cassandraSchema.name, tenantId);

        Map<ColumnIdentifier, FieldVector> vectorMap = ArrowToSSTable.alignSchemas(root, cassandraSchema);
        Path sstableDir = Files.createTempDirectory("sstables-");
        Descriptor writtenSSTableDescriptor = SSTableWriterUtils.writeCqlSSTable(cassandraSchema, vectorMap, sstableDir, true);
        List<SSTableData> ssTableData = SSTableWriterUtils.getSSTableData(writtenSSTableDescriptor.getDirectory());

        String dataComponentPath = BulkImporter.uploadSSTables(s3Admin, tenantId, DescriptorUtils.addKeyspaceAndTable(sstableDir, cassandraSchema), cassandraSchema, archive);
        for (SSTableData datum : ssTableData)
        {
            datum.fileName = Paths.get(dataComponentPath).getFileName().toString();
            datum.operationId = java.util.Optional.empty();
        }
        return ssTableData;
    }

    /**
     * Load data from the given bucket into Astra.
     *
     * @param tenantId the tenant ID, including the suffixed ordinal.
     * @param dataBucket The name of the bucket that the data is uploaded to. The script will look for Parquet files
     * under the prefix in ArrowToSSTable.
     * @param noArchive Whether to upload the files individually or as a zipped archive.
     * @param pretty Whether to pretty-print the JSON output.
     * @param skipExtras Whether to skip columns that are in the Parquet table but not the Cassandra table.
     *
     * @return indicator of success
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @javax.ws.rs.Path("/import/{tenantId}/{dataBucket}")
    public Response importData(
            @PathParam("tenantId") String tenantId,
            @PathParam("dataBucket") String dataBucket,
            @QueryParam("noArchive") boolean noArchive,
            @QueryParam("pretty") boolean pretty,
            @QueryParam("skipExtras") boolean skipExtras) throws JsonProcessingException
    {
        List<S3Object> objects;
        try
        {
            objects = ArrowToSSTable.listParquetFiles(s3, dataBucket);
        }
        catch (S3Exception e)
        {
            return Response.serverError()
                    .entity("Error fetching parquet files: " + e.awsErrorDetails()
                            .errorMessage())
                    .build();
        }

        Path outdir;
        try
        {
            if (System.getProperty("cndb.bulkimport.outdir") != null)
            {
                outdir = Paths.get(System.getProperty("cndb.bulkimport.outdir"));
            }
            else
            {
                outdir = Files.createTempDirectory("bulkimport-");
            }
        }
        catch (IOException e)
        {
            return Response.serverError()
                    .entity("Error creating temp directory: " + e.getMessage())
                    .build();
        }

        JSONObject schemaFile;
        try
        {
            schemaFile = BulkImporter.fetchSchemaFile(s3Admin, tenantId);
        }
        catch (IOException e)
        {
            return Response.serverError()
                    .entity("Error reading schema file: " + e.getMessage())
                    .build();
        }
        catch (ParseException e)
        {
            return Response.serverError()
                    .entity("Error parsing schema file: " + e.getMessage())
                    .build();
        }

        List<BulkImportTaskResult> taskResults = objects.stream()
                .flatMap(object -> handleParquetObject(dataBucket, object, schemaFile, tenantId, outdir, !noArchive).stream())
                .collect(Collectors.toList());

        if (pretty)
            return Response.ok(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(taskResults)).build();
        return Response.ok(OBJECT_MAPPER.writeValueAsString(taskResults)).build();
    }
}
