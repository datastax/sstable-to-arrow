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
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.service.StorageService;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

public class SSTableWriterTest {
    public static void main(String[] args) throws Exception {
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
                Object[][] objects = readArrowFile(uri);
                System.out.println("writing to SSTable");
                writeCqlSSTable(objects);
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

            Path dir = Files.createTempDirectory("download-");

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

    public static Object[][] readArrowFile(String path) throws Exception {
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

        Object[][] items = new Object[root.getRowCount()][fields.size()];
        System.out.println(root.getRowCount());
        System.out.println(fields.size());
        for (int i = 0; i < vectors.size(); i++) {
            for (int j = 0; j < root.getRowCount(); j++) {
                items[j][i] = vectors.get(i).getObject(j);
            }
        }

        AutoCloseables.close(batches);
        AutoCloseables.close(factory, dataset, scanner);

        return items;
    }

    public static void init() throws IOException {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    public static void writeCqlSSTable(Object[][] rows) throws IOException {
        String keyspace = "baselines";
        String table = "keyvalue";

        File dataDir = new File(System.getProperty("user.dir") + File.separator + "data" + File.separator + keyspace + File.separator + table);
        assert dataDir.exists();
        System.out.println("writing to " + dataDir.getAbsolutePath());

//        String schema = "CREATE TABLE baselines.iot (\n" +
//                "    machine_id uuid,\n" +
//                "    sensor_name text,\n" +
//                "    time timestamp,\n" +
//                "    data text,\n" +
//                "    sensor_value double,\n" +
//                "    station_id uuid,\n" +
//                "    PRIMARY KEY ((machine_id, sensor_name), time)\n" +
//                ")";
//        String insert = "INSERT INTO baselines.iot (machine_id , sensor_name , time , data , sensor_value , station_id ) VALUES ( ? , ? , ? , ? , ? , ? )";
        String schema = "CREATE TABLE baselines.keyvalue (\n" +
                "    key bigint PRIMARY KEY,\n" +
                "    value bigint\n" +
                ")";
        String insert = "INSERT INTO baselines.keyvalue (key, value) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(dataDir)
                .forTable(schema)
                .using(insert)
                .build();

        for (Object[] row : rows) {
            writer.addRow(row);
        }

        writer.close();
    }
}
