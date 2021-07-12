import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.KeyspaceParams;
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

public class SSTableLoaderTest {
    //    replace this with the absolute base path where your SSTables are kept
    //    i.e. the SSTable files (Data.db, Rows.db, etc) should be located at /<basePath>/<KEYSPACE1>/<CF_STANDARD>
    public static final String basePath = System.getProperty("user.dir") + File.separator + "resources";
    public static final String KEYSPACE1 = "baselines";
    public static final String CF_STANDARD1 = "iot";
    public static String sstablePath;

    public static void main(String[] args) throws Exception {
        if (args.length < 1)
        {
            System.out.println("must pass path to folder with SSTable files");
            return;
        }

        sstablePath = args[0];

        long startTime = System.nanoTime();

        init();

        List<FilteredPartition> partitions = getPartitions();

        ArrowTransferUtil util = new ArrowTransferUtil(partitions);
        util.process();

        StorageService.instance.shutdownServer();
        System.out.println("[PROFILE] done all: " + (System.nanoTime() - startTime));
        System.exit(0);
    }

    public static List<FilteredPartition> getPartitions() throws Exception {
//        File dataDir = dataDir(KEYSPACE1, CF_STANDARD1);
        File dataDir = new File(sstablePath);

//        if (dataDir.listFiles().length == 0) {
//            String schema = "CREATE TABLE %s.%s (key ascii, name ascii, val ascii, val1 ascii, PRIMARY KEY (key, name))";
//            String query = "INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)";
//
//            try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
//                    .inDirectory(dataDir)
//                    .forTable(String.format(schema, KEYSPACE1, CF_STANDARD1))
//                    .using(String.format(query, KEYSPACE1, CF_STANDARD1))
//                    .build()) {
//                writer.addRow("key1", "col1", "100");
//            }
//        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs.forceBlockingFlush(); // wait for sstables to be on disk else we won't be able to stream them

        final CountDownLatch latch = new CountDownLatch(1);
        long startTime = System.nanoTime();
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();
        List<FilteredPartition> partitions = getAll(cmd(cfs).build());
        latch.await();
        System.out.println("[PROFILE] done getting partitions: " + (System.nanoTime() - startTime));

        return partitions;
    }

    // utility stuff
    public static void init() {
        SchemaLoader.prepareServer();

//        	machine_id uuid,
//	sensor_name text,
//	time timestamp,
//	data text,
//	sensor_value double,
//	station_id uuid,
//	PRIMARY KEY ((machine_id, sensor_name), time))
        CFMetaData cfm = CFMetaData.Builder.createDense(KEYSPACE1, CF_STANDARD1, false, false)
                .addPartitionKey("machine_id", UUIDType.instance)
                .addPartitionKey("sensor_name", UTF8Type.instance)
                .addClusteringColumn("time", TimestampType.instance)
                .addRegularColumn("data", UTF8Type.instance)
                .addRegularColumn("sensor_value", DoubleType.instance)
                .addRegularColumn("station_id", UUIDType.instance)
                .build();

        SchemaLoader.createKeyspace(KEYSPACE1,
                KeyspaceParams.simple(1),
                cfm);

        StorageService.instance.initServer();
    }
    public static final class TestClient extends SSTableLoader.Client {
        private String keyspace;

        public void init(String keyspace) {
            this.keyspace = keyspace;
            for (Range<Token> range : StorageService.instance.getLocalRanges(KEYSPACE1))
                addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
        }

        public CFMetaData getTableMetadata(String tableName) {
            return Schema.instance.getCFMetaData(keyspace, tableName);
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
             PartitionIterator iterator = command.executeInternal(executionController)) {
            while (iterator.hasNext()) {
                try (RowIterator partition = iterator.next()) {
                    results.add(FilteredPartition.create(partition));
                }
            }
        }
        return results;
    }
    public static File dataDir(String ks, String cf) {
//        File tmpdir = Files.createTempDir();
        String pathname = basePath + File.separator + ks + File.separator + cf;

//        Java 8 solution
        try {
            Files.createDirectories(Paths.get(pathname));
            return new File(pathname);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}