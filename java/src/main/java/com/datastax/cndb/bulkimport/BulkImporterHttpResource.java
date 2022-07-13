package com.datastax.cndb.bulkimport;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.POST;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cndb.metadata.backup.BulkImportTaskSpec;
import com.datastax.sstablearrow.ArrowToSSTable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
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

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module());

    /**
     * Load data from the given bucket into Astra.
     *
     * @param tenantId the tenant ID, including the suffixed ordinal.
     * @param dataBucket The name of the bucket that the data is uploaded to. The script will look for Parquet files
     * under the prefix in ArrowToSSTable.
     * @param regionStr the region of the data bucket
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
            @QueryParam("region") String regionStr,
            @QueryParam("noArchive") boolean noArchive,
            @QueryParam("pretty") boolean pretty,
            @QueryParam("skipExtras") boolean skipExtras) throws JsonProcessingException
    {
        Region region;
        try {
            region = Region.of(regionStr);
        } catch (Throwable e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(OBJECT_MAPPER.writeValueAsString(new JSONObject().put("error", "Invalid region: " + regionStr)))
                    .build();
        }
        S3Client s3Client = BulkImportFileUtils.instance.s3Client(region);

        List<S3Object> objects;
        try
        {
            objects = ArrowToSSTable.listParquetFiles(s3Client, dataBucket);
        }
        catch (S3Exception e)
        {
            return Response.serverError()
                    .entity("Error fetching parquet files: " + e.awsErrorDetails()
                            .errorMessage())
                    .build();
        }

        JSONObject schemaFile;
        try
        {
            schemaFile = BulkImporter.fetchSchemaFile(tenantId);
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
                .flatMap(object -> {

                    BulkImportTaskSpec taskSpec = new BulkImportTaskSpec(
                            Collections.singleton(tenantId), URI.create("s3://" + dataBucket + "/" + object.key()), region, !noArchive);

                    return BulkImporter.handleParquetObject(taskSpec, schemaFile).stream();
                })
                .collect(Collectors.toList());

        if (pretty)
            return Response.ok(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(taskResults)).build();
        return Response.ok(OBJECT_MAPPER.writeValueAsString(taskResults)).build();
    }
}
