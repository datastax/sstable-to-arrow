import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SSTableWriterTest {
    public static String keyspace = "baselines", table = "keyvalue";
    public static String schemaStatement = "CREATE TABLE " + keyspace + "." + table + "(\n" +
            "    key bigint PRIMARY KEY,\n" +
            "    value bigint\n" +
            ")";
    public static String insertStatement = "INSERT INTO " + keyspace + "." + table + " (key, value) VALUES (?, ?)";

//    public static String schema = "CREATE TABLE baselines.iot (\n" +
//            "    machine_id uuid,\n" +
//            "    sensor_name text,\n" +
//            "    time timestamp,\n" +
//            "    data text,\n" +
//            "    sensor_value double,\n" +
//            "    station_id uuid,\n" +
//            "    PRIMARY KEY ((machine_id, sensor_name), time)\n" +
//            ")";
//    public static String insert = "INSERT INTO baselines.iot (machine_id , sensor_name , time , data , sensor_value , station_id ) VALUES ( ? , ? , ? , ? , ? , ? )";

    public static void main(String[] args) throws Exception {

        TableMetadata metadata = TableMetadata
                .builder(keyspace, table)
                .addPartitionKeyColumn("key", LongType.instance)
                .addRegularColumn("value", LongType.instance)
//                .addPartitionKeyColumn("machine_id", UUIDType.instance)
//                .addPartitionKeyColumn("sensor_name", UTF8Type.instance)
//                .addClusteringColumn("time", TimestampType.instance)
//                .addRegularColumn("data", UTF8Type.instance)
//                .addRegularColumn("sensor_value", DoubleType.instance)
//                .addRegularColumn("station_id", UUIDType.instance)
                .build();

        System.out.println("columns:");
        for (ColumnMetadata c : metadata.columns()) {
            System.out.println(c.name);
        }

        System.setProperty("cassandra.system_view.only_local_and_peers_table", "true");

        init();

        S3Client s3 = S3Client.builder()
                .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
                .region(Region.US_WEST_2)
                .build();

        String bucket = "astra-byob-dev";

        List<Path> paths = listBuckets(s3, bucket);

        paths.forEach(path -> {
            try {
                String uri = path.toUri().toString();
                System.out.println("reading from " + uri);
                VectorSchemaRoot root = readParquetFile(uri);
                Map<ColumnIdentifier, FieldVector> aligned = alignSchemas(root, metadata);
                System.out.println("writing to SSTable");
                writeCqlSSTable(metadata, aligned);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static List<Path> listBuckets(S3Client s3, String bucketName) {
        List<Path> outputs = new ArrayList<>();

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            Path dir = Files.createTempDirectory("download-s3-");

            for (ListIterator it = objects.listIterator(); it.hasNext(); ) {
                S3Object value = (S3Object) it.next();
                System.out.println(value.key());

                if (value.key().endsWith("parquet")) {
                    File file = new File(dir + File.separator + value.key());
                    assert Files.createDirectories(file.getParentFile().toPath())
                            .toFile().exists();

                    GetObjectRequest getObject = GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(value.key())
                            .build();

                    s3.getObject(getObject, file.toPath());

                    outputs.add(file.toPath());
                }
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return outputs;
    }

    public static VectorSchemaRoot readParquetFile(String path) throws Exception {
        BufferAllocator allocator = new RootAllocator();

        DatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, path);
        Dataset dataset = factory.finish();
        Scanner scanner = dataset.newScan(new ScanOptions(100));
        List<ArrowRecordBatch> batches = new ArrayList<>();
        for (ScanTask task : scanner.scan()) {
            for (ScanTask.BatchIterator it = task.execute(); it.hasNext(); ) {
                ArrowRecordBatch batch = it.next();
                batches.add(batch);
            }
        }

        List<Field> fields = factory.inspect().getFields();
        List<FieldVector> vectors = fields.stream().map(field -> field.createVector(allocator)).collect(Collectors.toList());
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        VectorLoader loader = new VectorLoader(root);
        batches.forEach(batch -> loader.load(batch));

        AutoCloseables.close(batches);
        AutoCloseables.close(factory, dataset, scanner);

        return root;
    }

    public static Map<ColumnIdentifier, FieldVector> alignSchemas(VectorSchemaRoot root, TableMetadata cassandraMetadata) {
        Map<ColumnIdentifier, FieldVector> positionMapping = new HashMap<>();
        List<FieldVector> vectors = root.getFieldVectors();

        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            ColumnMetadata col = cassandraMetadata.getColumn(ByteBuffer.wrap(vector.getName().getBytes()));
            if (col == null) {
                System.err.println("column " + vector.getName() + " not found in Cassandra table, skipping");
            } else if (!ArrowTransferUtil.validType(col.type, vector.getMinorType())) {
                System.err.printf("types for column %s don't match (CQL3Type %s (matches %s) != MinorType %s)\n",
                        col.name, col.type.asCQL3Type().toString(), ArrowTransferUtil.matchingType(col.type), vector.getMinorType());
            } else {
                positionMapping.put(col.name, vector);
            }
        }

        String[] mismatch = cassandraMetadata.columns().stream().map(columnMetadata -> {
            if (!positionMapping.containsKey(columnMetadata.name)) {
                return columnMetadata.name.toString();
            }
            return null;
        }).filter(col -> col != null).collect(Collectors.toList()).toArray(new String[0]);

        if (mismatch.length > 0) {
            throw new RuntimeException("no matching columns found in the Parquet file for Cassandra columns " + Arrays.toString(mismatch));
        }

        return positionMapping;
    }

    public static void init() throws IOException {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    public static void writeCqlSSTable(TableMetadata metadata, Map<ColumnIdentifier, FieldVector> data) throws IOException {
        File dataDir = new File(System.getProperty("user.dir") + File.separator + "data" + File.separator + keyspace + File.separator + table);
        assert dataDir.exists();
        System.out.println("writing to " + dataDir.getAbsolutePath());

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(dataDir)
                .forTable(schemaStatement)
                .using(insertStatement)
                .build();

        int size = data.values().stream().findFirst().get().getValueCount();

        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < size; i++) {
            row.clear();
            for (ColumnMetadata columnMetadata : metadata.columns()) {
                row.put(columnMetadata.name.toString(), data.get(columnMetadata.name).getObject(i));
            }
            writer.addRow(row);
        }

        writer.close();
    }
}
