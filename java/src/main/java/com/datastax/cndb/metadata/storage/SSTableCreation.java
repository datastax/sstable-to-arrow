package com.datastax.cndb.metadata.storage;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.compaction.OperationType;

/**
 * A class for grouping details that explain how an sstable was created.
 * Normally, this is the type and ID of the transaction. If there was
 * no transaction, then the ID is missing.
 * */
public class SSTableCreation
{
    /**
     * Operation type used to create current {@link SSTableData}, eg. FLUSH, COMPACTION, REGION_BOOTSTRAP, RESTORE, etc.
     */
    public final OperationType operationType;

    /**
     * This is the unique UUID of the operation that generated this sstable. This is the same
     * as the transaction id or the compaction task id. If no transaction was available, then this
     * is empty.
     */
    public final Optional<UUID> operationId;

    /**
     * True if the sstable data has multiple copies because compaction has not run yet, eg. FLUSH data.
     * This field is needed to calculate proper live user data size after region bootstrap or restore.
     * In BucketWalker, it divides replicated sstable size by replication factor.
     */
    public final boolean replicated;

    private SSTableCreation(OperationType operationType, Optional<UUID> operationId, boolean replicated)
    {
        this.operationType = operationType;
        this.operationId = operationId;
        this.replicated = replicated;
    }

    public static SSTableCreation withId(OperationType operationType, UUID operationId)
    {
        return withId(operationType, Optional.of(operationId), isReplicatedType(operationType));
    }

    public static SSTableCreation withId(OperationType operationType, Optional<UUID> operationId)
    {
        return withId(operationType, operationId, isReplicatedType(operationType));
    }

    public static SSTableCreation withId(OperationType operationType, Optional<UUID> operationId, boolean replicated)
    {
        return new SSTableCreation(operationType, operationId, replicated);
    }

    public static SSTableCreation fromExisting(SSTableData ssTableData)
    {
        Objects.requireNonNull(ssTableData, "Expected non-null existing sstable data");
        return new SSTableCreation(ssTableData.operationType, ssTableData.operationId, ssTableData.replicated);
    }

    public static SSTableCreation withoutId(OperationType operationType)
    {
        return new SSTableCreation(operationType, Optional.empty(), isReplicatedType(operationType));
    }

    /**
     * This is a utility method for tests, production call uses the transaction id or no id at all.
     */
    @VisibleForTesting
    public static SSTableCreation withRandomId(OperationType operationType)
    {
        return withRandomId(operationType, isReplicatedType(operationType));
    }

    /**
     * This is a utility method for tests, production call uses the transaction id or no id at all.
     */
    @VisibleForTesting
    public static SSTableCreation withRandomId(OperationType operationType, boolean replicated)
    {
        return new SSTableCreation(operationType, Optional.of(UUID.randomUUID()), replicated);
    }

    static boolean isReplicatedType(OperationType type)
    {
        return type == OperationType.FLUSH;
    }
}
