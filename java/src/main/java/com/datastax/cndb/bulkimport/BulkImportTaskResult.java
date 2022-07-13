package com.datastax.cndb.bulkimport;

import java.util.Objects;
import java.util.Optional;

import com.datastax.cndb.metadata.storage.SSTableData;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BulkImportTaskResult
{
    // the key of the parquet file in the user bucket
    @JsonProperty("object_key")
    public String objectKey;

    @JsonProperty("status")
    public String status;

    @JsonProperty("message")
    public String message;

    @JsonProperty("data")
    public Optional<SSTableData> data;

    private BulkImportTaskResult(
            @JsonProperty("object_key") String objectKey,
            @JsonProperty("status") String status,
            @JsonProperty("message") String message,
            @JsonProperty("data") Optional<SSTableData> data)
    {
        this.objectKey = Objects.requireNonNull(objectKey);
        this.status = Objects.requireNonNull(status);
        this.message = message;
        this.data = data;
    }

    public static BulkImportTaskResult success(String objectKey, SSTableData data)
    {
        return new BulkImportTaskResult(objectKey, "success", null, Optional.ofNullable(data));
    }

    public static BulkImportTaskResult error(String objectKey, String message)
    {
        return new BulkImportTaskResult(objectKey, "error", message, Optional.empty());
    }
}
