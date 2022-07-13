package com.datastax.cndb.bulkimport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Utilities for storing where downloaded files are written to on disk.
 */
public class BulkImportFileUtils
{

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportFileUtils.class);
    private final Path baseDir;

    private Map<Region, S3Client> regionalClients = new HashMap<>();
    private static final Region CNDB_REGION = Region.US_EAST_1;

    private final String s3ProfileName;

    public static final BulkImportFileUtils instance = new BulkImportFileUtils();

    public BulkImportFileUtils()
    {
        if (System.getProperty("cndb.bulkimport.outdir") != null)
        {
            baseDir = Paths.get(System.getProperty("cndb.bulkimport.outdir"));
        }
        else
        {
            try
            {
                baseDir = Files.createTempDirectory("bulkimport-");
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error initializing temporary directory: " + e.getMessage());
            }
        }
        if (!parquetDir().toFile().mkdirs() || !sstableDir().toFile().mkdirs())
        {
            throw new RuntimeException("Error creating directories: " + parquetDir() + ", " + sstableDir());
        }
        LOGGER.info("Bulk import files will be written to {}", baseDir);

        if (System.getProperty("cndb.bulkimport.s3profile") != null)
        {
            s3ProfileName = System.getProperty("cndb.bulkimport.s3profile");
        }
        else
        {
            s3ProfileName = "default";
        }
        LOGGER.info("Using credentials profile {}", s3ProfileName);
    }

    public S3Client s3Client(Region region)
    {
        if (!regionalClients.containsKey(region))
        {
            ProfileCredentialsProvider provider = ProfileCredentialsProvider.builder().profileName(s3ProfileName).build();
            S3Client client = S3Client.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
            regionalClients.put(region, client);
        }
        return regionalClients.get(region);
    }

    public S3Client getCndbClient() {
        return s3Client(CNDB_REGION);
    }

    public Path getBaseDir()
    {
        return baseDir;
    }

    public Path parquetDir()
    {
        return baseDir.resolve("parquets");
    }

    public Path sstableDir()
    {
        return baseDir.resolve("sstables");
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
