import com.datastax.sstablearrow.SSTableToArrow;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

class SSTableToArrowTest {

    String KEYSPACE1 = "baselines";
    String CF_STANDARD1 = "iot";

    String sstablePath = System.getProperty("user.dir") + File.separator + "resources" + File.separator + "data";

    @BeforeAll
    void init() {
        SchemaLoader.prepareServer();

        TableMetadata cfm = TableMetadata.builder(KEYSPACE1, CF_STANDARD1)
                .addPartitionKeyColumn("machine_id", UUIDType.instance)
                .addPartitionKeyColumn("sensor_name", UTF8Type.instance)
                .addClusteringColumn("time", TimestampType.instance)
                .addRegularColumn("data", UTF8Type.instance)
                .addRegularColumn("sensor_value", DoubleType.instance)
                .addRegularColumn("station_id", UUIDType.instance)
                .build();

        KeyspaceMetadata schema = KeyspaceMetadata.create(KEYSPACE1, KeyspaceParams.simple(1), Tables.of(cfm));
        SchemaLoader.doSchemaChanges(SchemaTransformations.createKeyspaceIfNotExists(schema));

        StorageService.instance.initServer();
    }

    void createDemoTable() {
        File dataDir = new File(sstablePath + File.separator + KEYSPACE1 + File.separator + CF_STANDARD1);
        if (dataDir.listFiles().length == 0) {
            String schema = "CREATE TABLE %s.%s (key ascii, name ascii, val ascii, val1 ascii, PRIMARY KEY (key, name))";
            String query = "INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)";

            try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                    .inDirectory(dataDir)
                    .forTable(String.format(schema, KEYSPACE1, CF_STANDARD1))
                    .using(String.format(query, KEYSPACE1, CF_STANDARD1))
                    .build()) {
                writer.addRow("key1", "col1", "100");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void getPartitions() {
        try {
            long startTime = System.nanoTime();
            List<FilteredPartition> partitions = SSTableToArrow.getPartitions(sstablePath, KEYSPACE1, CF_STANDARD1);
            for (FilteredPartition partition : partitions) {
                System.out.println(partition.toString());
            }
            System.out.println("[PROFILE] done all: " + (System.nanoTime() - startTime));
        } catch (Exception err) {
            System.err.println(err.getMessage());
        }
    }
}