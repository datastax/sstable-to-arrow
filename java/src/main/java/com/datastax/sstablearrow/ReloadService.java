package com.datastax.sstablearrow;

import com.datastax.cndb.metadata.storage.SSTableData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@javax.ws.rs.Path("/service")
public class ReloadService {

    private static final S3Client s3 = S3Client.builder()
//            .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
            .region(Region.US_WEST_2)
            .build();

    private static final S3Client s3Admin = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("bonus"))
            .region(Region.US_EAST_1)
            .build();

    @GET
    @javax.ws.rs.Path("/hello")
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello world";
    }

    /**
     * Load data from the given bucket into Astra.
     *
     * @param tenantId   the tenant ID, including the suffixed ordinal.
     * @param dataBucket The name of the bucket that the data is uploaded to. The script will look for Parquet files
     *                   under the prefix in ArrowToSSTable.
     * @return indicator of success
     */
    @POST
    @javax.ws.rs.Path("/import/{tenantId}/{dataBucket}")
    public Response importData(@PathParam("tenantId") String tenantId, @PathParam("dataBucket") String dataBucket, @QueryParam("archive") boolean archive) {
        List<Path> paths;
        try {
            paths = ArrowToSSTable.downloadAllParquetFiles(s3, dataBucket);
        } catch (S3Exception e) {
            return Response.serverError()
                    .entity("Error fetching parquet files: " + e.awsErrorDetails()
                            .errorMessage())
                    .build();
        } catch (IOException e) {
            return Response.serverError()
                    .entity("Error fetching parquet files: " + e.getMessage())
                    .build();
        }
        JSONObject schemaFile;
        try {
            schemaFile = ArrowToSSTable.fetchSchemaFile(s3Admin, tenantId);
        } catch (IOException e) {
            return Response.serverError()
                    .entity("Error reading schema file: " + e.getMessage())
                    .build();
        } catch (ParseException e) {
            return Response.serverError()
                    .entity("Error parsing schema file: " + e.getMessage())
                    .build();
        }

        List<String> failedUploads = new ArrayList<>();
        List<String> succeededUploads = new ArrayList<>();

        for (Path path : paths) {
            List<SSTableData> ssTableData;
            String dataComponentPath;

            try (VectorSchemaRoot root = ArrowToSSTable.readParquetFile("file:" + path.toString())) {
                Pair<String, String> keyspaceAndTableName = ArrowToSSTable.getKeyspaceAndTableName(path);
                String tableId = keyspaceAndTableName.left + "." + keyspaceAndTableName.right;
                TableMetadata metadata = ArrowToSSTable.getSchema(schemaFile, keyspaceAndTableName.left, keyspaceAndTableName.right);
                if (metadata == null) {
                    failedUploads.add("Error at " + path + ": Error getting Cassandra schema for table " + tableId);
                    continue;
                }

                Map<ColumnIdentifier, FieldVector> vectorMap;
                try {
                    vectorMap = ArrowToSSTable.alignSchemas(root, metadata);
                } catch (RuntimeException e) {
                    failedUploads.add("Error at " + path + ": Error aligning schemas for table " + tableId + ": " + e.getMessage());
                    continue;
                }

                Path sstableDir;
                try {
                    sstableDir = Files.createTempDirectory("sstables-");
                } catch (RuntimeException e) {
                    failedUploads.add("Error at " + path + ": Couldn't create temp directory for SSTables");
                    continue;
                }

                try {
                    ssTableData = ArrowToSSTable.writeCqlSSTable(metadata, vectorMap, sstableDir, true);
                } catch (Exception e) {
                    failedUploads.add("Error at " + path + ": Error writing produced SSTables to disk: " + e.getMessage());
                    continue;
                }

                try {
                    dataComponentPath = uploadSSTables(s3Admin, tenantId, Util.addKeyspaceAndTable(sstableDir, metadata), metadata, archive);
                } catch (Exception e) {
                    failedUploads.add("Error at " + path + ": Error uploading SSTables: " + e.getMessage());
                    continue;
                }
            } catch (Exception e) {
                failedUploads.add(String.format("Error at %s: Error reading Parquet file: %s\n", path.toAbsolutePath(), e.getMessage()));
                continue;
            }

            ObjectMapper mapper = new ObjectMapper();
            try {
                StringBuilder builder = new StringBuilder("[");
                for (SSTableData ssTableDatum : ssTableData) {
                    String sstableDataJson = mapper.writerFor(SSTableData.class)
                            .writeValueAsString(ssTableDatum);
                    if (builder.length() > 1) builder.append(", ");
                    builder.append(sstableDataJson);
                }
                builder.append("]");
                succeededUploads.add(path + ": written to path " + dataComponentPath + "; data: " + builder);
            } catch (JsonProcessingException e) {
                succeededUploads.add(path + ": written to path " + dataComponentPath + "; data: " + ssTableData.toString());
                continue;
            }
        }

        if (failedUploads.isEmpty()) {
            return Response.ok()
                    .build();
        }

        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("failed:\n");
        for (String failedUpload : failedUploads) {
            messageBuilder.append("- ");
            messageBuilder.append(failedUpload);
            messageBuilder.append('\n');
        }
        messageBuilder.append("\nsucceeded:\n");
        for (String succeededUpload : succeededUploads) {
            messageBuilder.append("- ");
            messageBuilder.append(succeededUpload);
            messageBuilder.append('\n');
        }

        return Response.serverError()
                .entity(messageBuilder.toString())
                .build();
    }

    private void compressFiles(List<Path> paths, Path outputPath, Descriptor descriptor) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(outputPath.toAbsolutePath()
                .toString())) {
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            for (int i = paths.size() - 1; i >= 0; i--) {
                Path path = paths.get(i);
                try (InputStream input = Files.newInputStream(path)) {
                    Component c = Descriptor.componentFromFilename(path.getFileName()
                            .toString());
//                    if (c == Component.STATS) continue;

//                    String fileName
                    ZipEntry zipEntry = new ZipEntry(descriptor.filenamePart() + "-" + c.toString());
                    zipOut.putNextEntry(zipEntry);

                    // write all other files to an archive
                    int nread;
                    byte[] buffer = new byte[1024];
                    while ((nread = input.read(buffer)) != -1) {
                        zipOut.write(buffer, 0, nread);
                    }
                }
            }
            zipOut.close();
        }
    }

    /**
     * @param s3           the client to make the upload request with
     * @param datacenterId the name of the bucket allocated for the tenant
     * @param sstableDir   the path to the local directory containing the SSTable files to upload
     * @param schema       the schema of the table to upload SSTables for
     * @return the descriptor for the remote SSTable
     */
    private String uploadSSTables(S3Client s3, String datacenterId, Path sstableDir, TableMetadata schema, boolean archive) throws IOException {
        String tenantId = ArrowToSSTable.getTenantId(datacenterId);
        String tableBucketPath = String.format("data/%s/%s/%s-%s/", tenantId, schema.keyspace, schema.name, schema.id.toHexString());

        Path archivePath = Files.createTempFile("sstable-archive-", ".zip");

        Path dataPath = null;
        List<Path> filePaths = new ArrayList<>();
        Descriptor descriptor = null;

//        get path to Data.db file and zip all others
        for (File sstableFile : Objects.requireNonNull(sstableDir.toFile()
                .listFiles())) {
            if (!Descriptor.isValid(sstableFile.toPath())) continue;

            Descriptor docDescriptor = Descriptor.fromFilename(sstableFile);
            if (descriptor != null && !docDescriptor.equals(descriptor)) {
                throw new RuntimeException(String.format("Documents have mismatched prefixes (%s != %s)", descriptor, docDescriptor));
            } else {
                descriptor = docDescriptor;
            }

            if (Descriptor.componentFromFilename(sstableFile).type == Component.Type.DATA) {
                dataPath = sstableFile.toPath();
            } else {
                filePaths.add(sstableFile.toPath());
            }
        }

        if (descriptor == null) {
            throw new FileNotFoundException("Additional SSTable files not found");
        }
        if (dataPath == null) {
            throw new FileNotFoundException("Data file not found");
        }

//        Create the new descriptor for remote storage
        Descriptor withUlid = Util.descriptorWithUlidGeneration(dataPath);
        if (archive) {
            compressFiles(filePaths, archivePath, withUlid);

//        upload Archive.zip file
            String archiveKey = tableBucketPath + withUlid.filenamePart() + "-Archive.zip";
            PutObjectRequest uploadArchive = PutObjectRequest.builder()
                    .bucket(datacenterId)
                    .key(archiveKey)
                    .build();
            s3.putObject(uploadArchive, archivePath);
            System.out.println("Successfully uploaded " + archivePath + " to " + archiveKey);
        } else {
//            upload each of the files
            for (Path filePath : filePaths) {
                Component component = Descriptor.componentFromFilename(filePath.getFileName()
                        .toString());
                String fileKey = tableBucketPath + withUlid.filenamePart() + "-" + component.toString();
                PutObjectRequest uploadFile = PutObjectRequest.builder()
                        .bucket(datacenterId)
                        .key(fileKey)
                        .build();
                s3.putObject(uploadFile, filePath);
                System.out.println("Successfully uploaded " + filePath + " to " + fileKey);
            }
        }

        //        upload Data.db file
        String dataKey = tableBucketPath + withUlid.pathFor(Component.DATA)
                .getFileName();
        PutObjectRequest putObject = PutObjectRequest.builder()
                .bucket(datacenterId)
                .key(dataKey)
                .build();
        s3.putObject(putObject, dataPath);
        System.out.println("Successfully uploaded " + dataPath + " to " + dataKey);

        return dataKey;
    }
}
