package com.datastax.cndb.bulkimport;

import java.util.List;
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
    public Optional<String> message;

    // sstables is a list since a single parquet file may correspond to several sstables
    @JsonProperty("data")
    public Optional<List<SSTableData>> data;

    private BulkImportTaskResult(
            @JsonProperty("object_key") String objectKey,
            @JsonProperty("status") String status,
            @JsonProperty("message") Optional<String> message,
            @JsonProperty("data") Optional<List<SSTableData>> data)
    {
        this.objectKey = Objects.requireNonNull(objectKey);
        this.status = Objects.requireNonNull(status);
        this.message = message;
        this.data = data;
    }

    public static BulkImportTaskResult success(String objectKey, List<SSTableData> data)
    {
        return new BulkImportTaskResult(objectKey, "success", Optional.empty(), Optional.of(data));
    }

    public static BulkImportTaskResult error(String objectKey, String message)
    {
        return new BulkImportTaskResult(objectKey, "error", Optional.of(message), Optional.empty());
    }
}
