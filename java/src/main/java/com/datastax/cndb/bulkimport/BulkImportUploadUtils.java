package com.datastax.cndb.bulkimport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.cndb.metadata.backup.BulkImportTaskSpec;
import com.datastax.sstablearrow.DescriptorUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static com.datastax.cndb.bulkimport.BulkImportFileUtils.compressFiles;
import static com.datastax.cndb.bulkimport.BulkImporter.getTenantId;

public class BulkImportUploadUtils
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportUploadUtils.class);

    /**
     * @param taskSpec the task spec
     * @param sstableDir the path to the local directory containing the SSTable files to upload
     * @param schema the schema of the table to upload SSTables for
     *
     * @return the descriptor for the remote SSTable
     */
    public static String uploadSSTables(BulkImportTaskSpec taskSpec, Path sstableDir, TableMetadata schema) throws IOException
    {
        LOGGER.debug("uploading SSTables at directory {} for table {}.{} under tenant {}",
                sstableDir, schema.keyspace, schema.name, taskSpec.getSingleTenant());

        String tenantId = getTenantId(taskSpec.getSingleTenant());
        String tableBucketPath = String.format("data/%s/%s/%s-%s/", tenantId, schema.keyspace, schema.name, schema.id.toHexString());

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
        if (taskSpec.isArchive())
        {
            Path archivePath = sstableDir.resolve("Archive.zip");
            compressFiles(filePaths, archivePath, withUlid);
            String archiveKey = tableBucketPath + withUlid.filenamePart() + "-Archive.zip";
            putFile(taskSpec, archiveKey, archivePath);
        }
        else
        {
            //  upload each of the files
            for (Path filePath : filePaths)
            {
                Component component = Descriptor.componentFromFilename(filePath.getFileName().toString());
                String destinationKey = tableBucketPath + withUlid.filenamePart() + "-" + component.toString();
                putFile(taskSpec, destinationKey, filePath);
            }
        }

        //  upload Data.db file
        String dataKey = tableBucketPath + withUlid.pathFor(Component.DATA).getFileName();
        putFile(taskSpec, dataKey, dataPath);

        return dataKey;
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
}
