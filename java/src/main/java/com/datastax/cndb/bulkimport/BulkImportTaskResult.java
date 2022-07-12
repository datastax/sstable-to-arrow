package com.datastax.cndb.bulkimport;

import java.util.Objects;
import java.util.Optional;

import com.datastax.cndb.metadata.storage.SSTableData;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BulkImportTaskResult
{
    @JsonProperty("path")
    public String path;

    @JsonProperty("status")
    public String status;

    @JsonProperty("message")
    public String message;

    @JsonProperty("data")
    public Optional<SSTableData> data;

    public BulkImportTaskResult(
            @JsonProperty("path") String path,
            @JsonProperty("status") String status,
            @JsonProperty("message") String message,
            @JsonProperty("data") Optional<SSTableData> data)
    {
        this.path = Objects.requireNonNull(path);
        this.status = Objects.requireNonNull(status);
        this.message = message;
        this.data = data;
    }

    public static BulkImportTaskResult success(String path, SSTableData data)
    {
        return new BulkImportTaskResult(path, "success", null, Optional.ofNullable(data));
    }

    public static BulkImportTaskResult error(String path, String message)
    {
        return new BulkImportTaskResult(path, "error", message, Optional.empty());
    }
}
