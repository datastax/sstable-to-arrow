package com.datastax.cndb.metadata.backup;

import java.net.URI;
import java.util.Set;

import com.datastax.cndb.bulkimport.BulkImportFileUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static java.util.Objects.requireNonNull;

/**
 * Defines a bulk import task for a single Parquet file.
 */
public class BulkImportTaskSpec
{

    @JsonProperty("tenants")
    private final Set<String> tenants;

    @JsonProperty("parquet_path")
    private final URI parquetPath;

    @JsonProperty("region")
    private final Region region;

    @JsonProperty("archive")
    private final boolean archive;

    public BulkImportTaskSpec(
            @JsonProperty("tenants") Set<String> tenants,
            @JsonProperty("parquet_path") URI parquetPath,
            @JsonProperty("region") Region region,
            @JsonProperty("archive") boolean archive)
    {
        this.tenants = requireNonNull(tenants);
        this.parquetPath = requireNonNull(parquetPath);
        this.region = requireNonNull(region);
        this.archive = requireNonNull(archive);
    }

    public String getSingleTenant()
    {
        return tenants.iterator().next();
    }

    @JsonIgnore
    public URI getFullURI()
    {
        return parquetPath;
    }

    @JsonIgnore
    public String getBucketName()
    {
        return parquetPath.getHost();
    }

    // get the key of the object in the bucket
    @JsonIgnore
    public String getObjectKey()
    {
        return parquetPath.getPath().substring(1);
    }

    @JsonIgnore
    public Region getRegion() {return region;}

    public boolean isArchive()
    {
        return archive;
    }

}
