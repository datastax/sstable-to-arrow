package com.datastax.cndb.bulkimport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.sstablearrow.DescriptorUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

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
     * @param s3 the S3 client
     * @param datacenterId the tenant ID and bucket name, a UUID with a suffixed ordinal
     */
    public static JSONObject fetchSchemaFile(S3Client s3, String datacenterId) throws IOException, ParseException
    {
        LOGGER.debug("fetching schema file for {}", datacenterId);

        String prefix = String.format("metadata_backup/%s/schema", getTenantId(datacenterId));
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(datacenterId)
                .prefix(prefix)
                .build();

        ListObjectsResponse res = s3.listObjects(listObjects);
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
        InputStream schemaFile = s3.getObjectAsBytes(getObject)
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
     * @param s3 the client to make the upload request with
     * @param datacenterId the name of the bucket allocated for the tenant
     * @param sstableDir the path to the local directory containing the SSTable files to upload
     * @param schema the schema of the table to upload SSTables for
     *
     * @return the descriptor for the remote SSTable
     */
    public static String uploadSSTables(S3Client s3, String datacenterId, Path sstableDir, TableMetadata schema, boolean archive) throws IOException
    {
        LOGGER.debug("uploading SSTables at directory {} for table {}.{} under tenant {}",
                sstableDir, schema.keyspace, schema.name, datacenterId);

        String tenantId = getTenantId(datacenterId);
        String tableBucketPath = String.format("data/%s/%s/%s-%s/", tenantId, schema.keyspace, schema.name, schema.id.toHexString());

        Path archivePath = Files.createTempFile("sstable-archive-", ".zip");

        Path dataPath = null;
        List<Path> filePaths = new ArrayList<>();
        Descriptor descriptor = null;

        // get path to Data.db file and zip all others
        for (File sstableFile : Objects.requireNonNull(sstableDir.toFile().listFiles()))
        {
            if (!Descriptor.isValid(sstableFile.toPath())) continue;

            Descriptor docDescriptor = Descriptor.fromFilename(sstableFile);
            if (descriptor != null && !docDescriptor.equals(descriptor))
            {
                throw new RuntimeException(String.format("Documents have mismatched prefixes (%s != %s)", descriptor, docDescriptor));
            }
            else
            {
                descriptor = docDescriptor;
            }

            if (Descriptor.componentFromFilename(sstableFile).type == Component.Type.DATA)
            {
                dataPath = sstableFile.toPath();
            }
            else
            {
                filePaths.add(sstableFile.toPath());
            }
        }

        if (descriptor == null)
        {
            LOGGER.error("No non-data SSTable files found in {}", sstableDir);
            throw new FileNotFoundException("Additional SSTable files not found");
        }
        if (dataPath == null)
        {
            LOGGER.error("No Data.db file found in {}", sstableDir);
            throw new FileNotFoundException("Data file not found");
        }

        // Create the new descriptor for remote storage
        Descriptor withUlid = DescriptorUtils.descriptorWithUlidGeneration(dataPath);
        if (archive)
        {
            compressFiles(filePaths, archivePath, withUlid);

            // upload Archive.zip file
            String archiveKey = tableBucketPath + withUlid.filenamePart() + "-Archive.zip";
            LOGGER.info("uploading archive file {} to bucket {} at key {}", archivePath, datacenterId, archiveKey);
            PutObjectRequest uploadArchive = PutObjectRequest.builder()
                    .bucket(datacenterId)
                    .key(archiveKey)
                    .build();
            s3.putObject(uploadArchive, archivePath);
            LOGGER.debug("upload complete");
        }
        else
        {
            //  upload each of the files
            for (Path filePath : filePaths)
            {
                Component component = Descriptor.componentFromFilename(filePath.getFileName().toString());
                String fileKey = tableBucketPath + withUlid.filenamePart() + "-" + component.toString();
                LOGGER.info("uploading file {} to bucket {} at key {}", filePath, datacenterId, fileKey);
                PutObjectRequest uploadFile = PutObjectRequest.builder()
                        .bucket(datacenterId)
                        .key(fileKey)
                        .build();
                s3.putObject(uploadFile, filePath);
                LOGGER.debug("upload complete");
            }
        }

        //  upload Data.db file
        String dataKey = tableBucketPath + withUlid.pathFor(Component.DATA).getFileName();
        PutObjectRequest putObject = PutObjectRequest.builder()
                .bucket(datacenterId)
                .key(dataKey)
                .build();
        LOGGER.info("uploading data file {} to bucket {} at key {}", dataPath, datacenterId, dataKey);
        s3.putObject(putObject, dataPath);
        LOGGER.info("upload complete");

        return dataKey;
    }

    public static void compressFiles(List<Path> paths, Path outputPath, Descriptor descriptor) throws IOException
    {
        LOGGER.debug("compressing {} files to {}", paths.size(), outputPath);

        try (OutputStream fos = Files.newOutputStream(outputPath))
        {
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            for (int i = paths.size() - 1; i >= 0; i--)
            {
                Path path = paths.get(i);
                try (InputStream input = Files.newInputStream(path))
                {
                    Component c = Descriptor.componentFromFilename(path.getFileName()
                            .toString());
                    //                    if (c == Component.STATS) continue;
                    ZipEntry zipEntry = new ZipEntry(descriptor.filenamePart() + "-" + c.toString());
                    zipOut.putNextEntry(zipEntry);

                    // write all other files to an archive
                    int nread;
                    byte[] buffer = new byte[1024];
                    while ((nread = input.read(buffer)) != -1)
                    {
                        zipOut.write(buffer, 0, nread);
                    }
                }
            }
            zipOut.close();
        }

        LOGGER.debug("compression complete");
    }
}
