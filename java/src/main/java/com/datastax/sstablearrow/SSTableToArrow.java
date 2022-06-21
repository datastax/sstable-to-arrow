package com.datastax.sstablearrow;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class SSTableToArrow {

    public static List<FilteredPartition> getPartitions(String sstablePath, String keyspace, String table) throws ExecutionException, InterruptedException {
        File dataDir = new File(sstablePath);

        ColumnFamilyStore cfs = Keyspace.open(keyspace)
                .getColumnFamilyStore(table);
        cfs.forceBlockingFlush(); // wait for sstables to be on disk else we won't be able to stream them

//        measure time from SSTableLoader creation
        long startTime = System.nanoTime();
        final CountDownLatch latch = new CountDownLatch(1);
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
//        the loader will countdown the latch when it's done reading the table
        loader.stream(Collections.emptySet(), completionStreamListener(latch))
                .get();
        List<FilteredPartition> partitions = getAll(cmd(cfs).build());
        latch.await();
        System.out.println("[PROFILE] done getting partitions: " + (System.nanoTime() - startTime));

        return partitions;
    }

    private static final class TestClient extends SSTableLoader.Client {
        private String keyspace;

        public void init(String keyspace) {
            this.keyspace = keyspace;
            for (Range<Token> range : StorageService.instance.getLocalRanges(keyspace))
                addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
        }

        public TableMetadataRef getTableMetadata(String tableName) {
            return SchemaManager.instance.getTableMetadataRef(keyspace, tableName);
        }
    }

    public static StreamEventHandler completionStreamListener(final CountDownLatch latch) {
        return new StreamEventHandler() {
            public void onFailure(Throwable arg0) {
                latch.countDown();
            }

            public void onSuccess(StreamState arg0) {
                latch.countDown();
            }

            public void handleStreamEvent(StreamEvent event) {
            }
        };
    }

    public static AbstractReadCommandBuilder.PartitionRangeBuilder cmd(ColumnFamilyStore cfs) {
        return new AbstractReadCommandBuilder.PartitionRangeBuilder(cfs);
    }

    public static List<FilteredPartition> getAll(ReadCommand command) {
        List<FilteredPartition> results = new ArrayList<>();
        try (ReadExecutionController executionController = command.executionController();
             PartitionIterator iterator = (PartitionIterator) command.executeInternal((Monitor) executionController)) {
            while (iterator.hasNext()) {
                try (RowIterator partition = iterator.next()) {
                    results.add(FilteredPartition.create(partition));
                }
            }
        }
        return results;
    }
}