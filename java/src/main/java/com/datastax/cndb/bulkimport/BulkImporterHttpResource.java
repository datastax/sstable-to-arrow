package com.datastax.cndb.bulkimport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
        List<Path> paths;
        try
        {
            paths = ArrowToSSTable.downloadAllParquetFiles(s3, dataBucket);
        }
        catch (S3Exception e)
        {
            return Response.serverError()
                    .entity("Error fetching parquet files: " + e.awsErrorDetails()
                            .errorMessage())
                    .build();
        }
        catch (IOException e)
        {
            return Response.serverError()
                    .entity("Error fetching parquet files: " + e.getMessage())
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

        List<BulkImportTaskResult> taskResults = new ArrayList<>();

        for (Path path : paths)
        {
            Pair<String, String> keyspaceAndTableName = ArrowToSSTable.getKeyspaceAndTableName(path);
            String tableId = keyspaceAndTableName.left + "." + keyspaceAndTableName.right;
            TableMetadata metadata = ArrowToSSTable.getSchema(schemaFile, keyspaceAndTableName.left, keyspaceAndTableName.right);
            if (metadata == null)
            {
                taskResults.add(BulkImportTaskResult.error(path.toString(), "Error getting Cassandra schema for table " + tableId));
                continue;
            }

            // each Parquet file may result in multiple SSTables
            try
            {
                ParquetReaderUtils.read("file:" + path, root -> {
                    try
                    {
                        List<BulkImportTaskResult> results = processVectorSchemaRoot(root, metadata, tenantId, !noArchive).stream()
                                .map(datum -> BulkImportTaskResult.success(path.toString(), datum))
                                .collect(Collectors.toList());
                        taskResults.addAll(results);
                    }
                    catch (Exception e)
                    {
                        taskResults.add(BulkImportTaskResult.error(path.toString(), "Error processing vector schema root: " + e.getMessage()));
                    }
                });
            }
            catch (Exception e)
            {
                taskResults.add(BulkImportTaskResult.error(path.toString(), "Error reading Parquet file: " + e.getMessage()));
            }
        }

        if (pretty)
            return Response.ok(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(taskResults)).build();
        return Response.ok(OBJECT_MAPPER.writeValueAsString(taskResults)).build();
    }

    public List<SSTableData> processVectorSchemaRoot(VectorSchemaRoot root, TableMetadata cassandraSchema, String tenantId, boolean archive) throws IOException, ExecutionException, InterruptedException
    {
        Map<ColumnIdentifier, FieldVector> vectorMap = ArrowToSSTable.alignSchemas(root, cassandraSchema);
        Path sstableDir = Files.createTempDirectory("sstables-");
        List<SSTableData> ssTableData = SSTableWriterUtils.writeCqlSSTable(cassandraSchema, vectorMap, sstableDir, true);
        String dataComponentPath = BulkImporter.uploadSSTables(s3Admin, tenantId, DescriptorUtils.addKeyspaceAndTable(sstableDir, cassandraSchema), cassandraSchema, archive);
        for (SSTableData datum : ssTableData)
        {
            datum.fileName = Paths.get(dataComponentPath).getFileName().toString();
            datum.operationId = java.util.Optional.empty();
        }
        return ssTableData;
    }
}
