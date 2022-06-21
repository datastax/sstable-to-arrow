package com.datastax.sstablearrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.cassandra.schema.TableMetadata;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
public class ReloadController {

    private static S3Client s3 = S3Client.builder()
//            .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
            .region(Region.US_WEST_2)
            .build();


    private static S3Client s3Admin = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("bonus"))
            .build();

    public static void main(String[] args) {
        SpringApplication.run(ReloadController.class, args);
    }

    /**
     * Load data from the given bucket into Astra.
     *
     * @param tenantId   the tenant ID, including the suffixed ordinal.
     * @param dataBucket The name of the bucket that the data is uploaded to. The script will look for Parquet files
     *                   under the prefix in ArrowToSSTable.
     * @return indicator of success
     */
    @PostMapping("/reload/{tenantId}/{dataBucket}")
    public String reload(@PathVariable("tenantId") String tenantId, @PathVariable("dataBucket") String dataBucket) {
        List<Path> paths = ArrowToSSTable.downloadAllParquetFiles(s3, dataBucket);
        Map<String, TableMetadata> schemaCache = new HashMap<>();
        for (Path path : paths) {
            String keyspaceName = path.getParent()
                    .getParent()
                    .getFileName()
                    .toString();
            String tableName = path.getParent()
                    .getFileName()
                    .toString();
//            Assume there are no periods in the keyspace or table name, or at least that concatenating them with a
//            period is unique
//            schemaCache.put(keyspaceName + "." + tableName,
//        );
            try (VectorSchemaRoot root = ArrowToSSTable.readParquetFile("file:" + path.toString())) {
                TableMetadata metadata = (TableMetadata) ArrowToSSTable.fetchSchema(s3Admin, tenantId, keyspaceName, tableName);
                ArrowToSSTable.alignSchemas(root, metadata);
            } catch (Exception e) {
                System.err.printf("Error reading Parquet file at %s: %s\n", path.toAbsolutePath(), e.getMessage());
            }
        }

        return String.format("tenantId=%s, bucketName=%s", tenantId, dataBucket);
    }
}
