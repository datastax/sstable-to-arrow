package com.datastax.cndb.metadata.storage;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * <ul>
 *   <li>Version 0: initial version
 *     <ul>
 *       <li>the path to the sstable, this is actually the key in etcd, so it is not serialized</li>
 *       <li>the sstable token range (min and max token). Here we assume that Tokens are Long
 *       tokens because this helps with performance and with deserializing data from out of C*
 *       services, e.g. by attaching a utility directly to etcd.</li>
 *       <li>the type of operation that created this sstable, i.e. flush or compaction, see {@link OperationType}</li>
 *       <li>the size of the data file in bytes</li>
 *     </ul>
 *   </li>
 *
 *   <li>Version 1: added the version field itself, and the metadata properties needed for compaction,
 *       see Javadoc inline for a list of fields, but briefly:
 *     <ul>
 *       <li>uncompressed length in bytes</li>
 *       <li>min and max timestamp</li>
 *       <li>min and max deletion times</li>
 *       <li>estimated number of keys</li>
 *     </ul>
 *   </li>
 *
 *   <li>Version 2: added "replicated" field which is used to compute proper user live data size.
 *     <ul>
 *       <li>replicated: if sstable contains replicated data, eg. flushed data from write</li>
 *     </ul>
 *   </li>
 *
 *   <li>Version 3: added "operationId" field which is the unique UUID of the operation that generated this sstable.
 *     <ul>
 *       <li>operationId: This is the same as the transaction id or the compaction task id; or empty if no transaction < 3</li>
 *     </ul>
 *   </li>
 *
 *   <li>Version 4: added "archiveSize" field which is used to compute live data size without checking s3
 *     <ul>
 *       <li>archiveSize: the size of archive file or 0 if it's from legacy sstable data where version < 4</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class SSTableData
{
    public static final int CURRENT_VERSION = 4;

    // The version was added with CNDB-1856, starting at 1, before that, it was implied as 0

    @JsonProperty
    public final int version;

    // The following properties have been available since the beginning (version 0 or no version)

    @JsonProperty("file_name")
    public final String fileName;

    @JsonProperty("min")
    public final long min; // should have been minToken

    @JsonProperty("max")
    public final long max; // should have been maxToken

    /**
     * Operation type used to create current {@link SSTableData}, eg. FLUSH, COMPACTION, REGION_BOOTSTRAP, RESTORE, etc.
     */
    @JsonProperty("operation_type")
    public final OperationType operationType;

    @JsonProperty("data_size")
    public final long dataSize;

    @JsonIgnore
    public final Range<Token> range;

    // The following properties have been available since version 1 (CDNB-1856)

    @JsonProperty("uncompressed_length")
    public final long uncompressedLength;

    @JsonProperty("min_timestamp")
    public final long minTimestamp;

    @JsonProperty("max_timestamp")
    public final long maxTimestamp;

    @JsonProperty("min_local_deletion_time")
    public final int minLocalDeletionTime;

    @JsonProperty("max_local_deletion_time")
    public final int maxLocalDeletionTime;

    @JsonProperty("estimated_keys")
    public final long estimatedKeys;

    // The following properties have been available since version 2 (CDNB-2582)

    /**
     * True if the sstable data has multiple copies because compaction has not run yet, eg. FLUSH data.
     * This field is needed to calculate proper live user data size after region bootstrap or restore.
     * In BucketWalker, it divides replicated sstable size by replication factor.
     */
    @JsonProperty("replicated")
    public final boolean replicated;

    // The following properties have been available since version 3 (CDNB-3626)

    /**
     * This is the unique UUID of the operation that generated this sstable. This is the same
     * as the transaction id or the compaction task id. If no transaction was available, then this
     * is empty.
     */
    @JsonProperty("operation_id")
    public final Optional<UUID> operationId;

    // The following properties have been available since version 4 (CDNB-3721)

    @JsonProperty("archive_size")
    public final long archiveSize;

    public SSTableData(
            @JsonProperty("version") int version,
            @JsonProperty("file_name") String fileName,
            @JsonProperty("min") long min,
            @JsonProperty("max") long max,
            @JsonProperty("operation_type") String operationType,
            @JsonProperty("data_size") long dataSize,
            @JsonProperty("uncompressed_length") long uncompressedLength,
            @JsonProperty("archive_size") long archiveSize,
            @JsonProperty("min_timestamp") long minTimestamp,
            @JsonProperty("max_timestamp") long maxTimestamp,
            @JsonProperty("min_local_deletion_time") int minLocalDeletionTime,
            @JsonProperty("max_local_deletion_time") int maxLocalDeletionTime,
            @JsonProperty("estimated_keys") long estimatedKeys,
            @JsonProperty("replicated") boolean replicated,
            @JsonProperty("operation_id") Optional<UUID> operationId)
    {
        this(version,
                fileName,
                min,
                max,
                OperationType.valueOf(operationType),
                dataSize,
                uncompressedLength,
                archiveSize,
                minTimestamp,
                maxTimestamp,
                minLocalDeletionTime,
                maxLocalDeletionTime,
                estimatedKeys,
                replicated,
                operationId);
    }

    public SSTableData(
            String fileName,
            long min,
            long max,
            long dataSize,
            long uncompressedLength,
            long archiveSize,
            long minTimestamp,
            long maxTimestamp,
            int minLocalDeletionTime,
            int maxLocalDeletionTime,
            long estimatedKeys,
            SSTableCreation ssTableCreation)
    {
        this(CURRENT_VERSION,
                fileName,
                min,
                max,
                dataSize,
                uncompressedLength,
                archiveSize,
                minTimestamp,
                maxTimestamp,
                minLocalDeletionTime,
                maxLocalDeletionTime,
                estimatedKeys,
                ssTableCreation);
    }

    public SSTableData(
            int version,
            String fileName,
            long min,
            long max,
            long dataSize,
            long uncompressedLength,
            long archiveSize,
            long minTimestamp,
            long maxTimestamp,
            int minLocalDeletionTime,
            int maxLocalDeletionTime,
            long estimatedKeys,
            SSTableCreation ssTableCreation)
    {
        this(version,
                fileName,
                min,
                max,
                ssTableCreation.operationType,
                dataSize,
                uncompressedLength,
                archiveSize,
                minTimestamp,
                maxTimestamp,
                minLocalDeletionTime,
                maxLocalDeletionTime,
                estimatedKeys,
                ssTableCreation.replicated,
                ssTableCreation.operationId);
    }

    private SSTableData(
            int version,
            String fileName,
            long min,
            long max,
            OperationType operationType,
            long dataSize,
            long uncompressedLength,
            long archiveSize,
            long minTimestamp,
            long maxTimestamp,
            int minLocalDeletionTime,
            int maxLocalDeletionTime,
            long estimatedKeys,
            boolean replicated,
            Optional<UUID> operationId)
    {
        this.version = version;
        this.fileName = fileName;
        this.min = min;
        this.max = max;
        this.operationType = operationType;
        // for version prior to cndb-2582, mark replicated if it's replicated type
        this.replicated = version < 2 ? SSTableCreation.isReplicatedType(operationType) : replicated;
        this.dataSize = dataSize;
        this.range = new Range<>(new Murmur3Partitioner.LongToken(min), new Murmur3Partitioner.LongToken(max));
        this.uncompressedLength = uncompressedLength;
        this.archiveSize = archiveSize;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.estimatedKeys = estimatedKeys;
        this.operationId = operationId;
    }

    public static SSTableData create(SSTableReader ssTableReader, SSTableCreation ssTableCreation)
    {
        String fileName = getFileName(ssTableReader);
        return new SSTableData(
                fileName,
                ssTableReader.getFirst().getToken().getLongValue(),
                ssTableReader.getLast().getToken().getLongValue(),
                ssTableReader.onDiskLength(),
                ssTableReader.uncompressedLength(),
//                MODIFIED
                FileUtils.size(ssTableReader.descriptor.pathFor(new Component(Component.Type.CUSTOM, "Archive.zip"))),
                ssTableReader.getMinTimestamp(),
                ssTableReader.getMaxTimestamp(),
                ssTableReader.getMinLocalDeletionTime(),
                ssTableReader.getMaxLocalDeletionTime(),
                ssTableReader.estimatedKeys(),
                ssTableCreation);
    }

    public static String getFileName(SSTableReader ssTableReader)
    {
        // We call getFileName() twice because ssTableReader.getFilename() is actually
        // returning the full path, the name of the method is misleading
        return ssTableReader.getFilename().getFileName().toString();
    }

    public SSTableData copyWithOperationType(OperationType type)
    {
        return new SSTableData(this.version, this.fileName, this.min, this.max, type, this.dataSize,
                this.uncompressedLength, this.archiveSize, this.minTimestamp, this.maxTimestamp, this.minLocalDeletionTime,
                this.maxLocalDeletionTime, this.estimatedKeys, this.replicated, this.operationId);
    }

    public SSTableData withDifferentCreation(SSTableCreation creation)
    {
        return new SSTableData(this.version, this.fileName, this.min, this.max, creation.operationType, this.dataSize,
                this.uncompressedLength, this.archiveSize, this.minTimestamp, this.maxTimestamp, this.minLocalDeletionTime,
                this.maxLocalDeletionTime, this.estimatedKeys, creation.replicated, creation.operationId);
    }

    @JsonIgnore
    public boolean needsUpgrading()
    {
        if (version >= SSTableData.CURRENT_VERSION)
            return false; // already at latest version

        if (version == 2 && SSTableData.CURRENT_VERSION == 3)
            return false; // upgrade not required for 2 -> 3 (operation id is lost anyway)

        return true;
    }
    /**
     * @return true if current sstable data has proper archive size or false if it's legacy sstable data
     */
    public boolean hasArchiveSize()
    {
        return version >= 4;
    }

    /**
     * @return true if the sstable data has multiple copies because compaction has not run yet, eg. FLUSH data.
     */
    @JsonIgnore
    public boolean isReplicated()
    {
        return replicated;
    }

    /**
     * @return true if the sstable is created by repair, eg. region continuous repair or repair decommission.
     */
    @JsonIgnore
    public boolean isRepaired()
    {
        return operationType == OperationType.REGION_REPAIR || operationType == OperationType.REGION_DECOMMISSION;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null || !(o instanceof SSTableData))
            return false;

        SSTableData that = (SSTableData) o;
        return version == that.version
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(operationType, that.operationType)
                && Objects.equals(replicated, that.replicated)
                && dataSize == that.dataSize
                && uncompressedLength == that.uncompressedLength
                && archiveSize == that.archiveSize
                && minTimestamp == that.minTimestamp
                && maxTimestamp == that.maxTimestamp
                && minLocalDeletionTime == that.minLocalDeletionTime
                && maxLocalDeletionTime == that.maxLocalDeletionTime
                && estimatedKeys == that.estimatedKeys
                && Objects.equals(operationId, that.operationId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, fileName, min, max, operationType, replicated, dataSize,
                uncompressedLength, archiveSize, minTimestamp, maxTimestamp, minLocalDeletionTime,
                maxLocalDeletionTime, estimatedKeys, operationId);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", "[", "]")
                .add("version: " + version)
                .add("fileName: " + fileName)
                .add("min: " + min)
                .add("max: " + max)
                .add("operationType: " + operationType)
                .add("operationId: " + operationId)
                .add("replicated: " + replicated)
                .add("dataSize: " + FBUtilities.prettyPrintMemory(dataSize))
                .add("uncompressedLength: " + FBUtilities.prettyPrintMemory(uncompressedLength))
                .add("archiveSize: " + FBUtilities.prettyPrintMemory(archiveSize))
                .add("minTimestamp: " + minTimestamp)
                .add("maxTimestamp: " + maxTimestamp)
                .add("minLocalDeletionTime: " + minLocalDeletionTime)
                .add("maxLocalDeletionTime: " + maxLocalDeletionTime)
                .add("estimatedKeys: " + estimatedKeys)
                .toString();
    }
}
