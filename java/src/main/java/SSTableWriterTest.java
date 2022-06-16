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
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

public class SSTableWriterTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("cassandra.system_view.only_local_and_peers_table", "true");

//        Object[][] rows = readArrowFile("s3://astra-byob-dev/keyvalue-000001.parquet");
//        for (Object[] row : rows) {
//            System.out.println(Arrays.toString(row));
//        }

//        init();
//        Object[] row = new Object[]{UUID.fromString("396f3d5d-8efa-444f-a809-b134beca04a8"),
//                "power_factor",
//                new Date(),
//                "rper at non lacus. Nulla eleifend facilisis pretium.\\nCurabitur vestibulum, nibh vel tempor condimentum, augue dolor finibus lorem, sed semper nunc nibh eu ligula. Phasellus consectetur erat nec condimentum dapibus. Ut aliquam commodo velit, vitae varius nunc pharetra eu. Vestibulum tincidunt laoreet dolor ut accumsan. Curabitur elit urna, accumsan et posuere vitae, maximus et enim. Suspendisse sit amet dui urna. Integer justo magna, elementum a erat ut, dignissim hendrerit mauris. Curabitur ac congue lacus. Interdum et malesuada fames ac ante ipsum primis in faucibus.\\nDonec interdum facilisis augue ut vestibulum. Donec convallis consequat odio ac consequat. Curabitur a ante quis neque posuere pulvinar ac et sem. Maecenas pellentesque rhoncus pulvinar. Phasellus vitae massa sed urna iaculis auctor. Suspendisse finibus ipsum in tellus molestie egestas. Pellentesque quam nibh, viverra id dui euismod, suscipit aliquam massa. Mauris in lacus suscipit, maximus felis vitae, tempor orci. Cras in velit a ex facilisis eleifend id a lorem. Aenean nec dolor at to",
//                100.4194,
//                UUID.fromString("17057725-3482-405a-8744-74686c06ddc1")
//        };

//        Object[][] rows = readArrowFile("file:///Users/alex.cai/Documents/data/keyvalue-100.csv/keyvalue-000001.parquet");
//        writeCqlSSTable(rows);
//
        S3Client s3 = S3Client.builder()
                .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
                .region(Region.US_WEST_2)
                .build();

        String bucket = "astra-byob-dev";
//
//        List<Bucket> buckets = s3.listBuckets().buckets();
//        System.out.println("S3 buckets:");
//        for (Bucket b : buckets) {
//            System.out.println("* " + b.name());
//        }

        listBuckets(s3, bucket);
    }

    public static void listBuckets(S3Client s3, String bucketName) {
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

                File file = new File(dir + File.separator + value.key());
                System.out.printf("writing file %s to dir %s - %s\n", value.key(), file.getParentFile().getPath(), file.getPath());
                Path parent = Files.createDirectories(file.getParentFile().toPath());
                System.out.println(parent.toFile().exists());
//                assert file.getParentFile().canWrite();
//                assert file.getParentFile().mkdirs();

                GetObjectRequest getObject = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(value.key())
                        .build();

                s3.getObject(getObject, file.toPath());
                System.out.println("DONE WRITING FILE");
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static Object[][] readArrowFile(String path) throws Exception {
        BufferAllocator allocator = new RootAllocator();

        DatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, path);
        Dataset dataset = factory.finish();
        Scanner scanner = dataset.newScan(new ScanOptions(100));
        List<ArrowRecordBatch> batches = new ArrayList<>();
//                StreamSupport.stream(
//                        scanner.scan().spliterator(), false)
//                .flatMap(t -> stream(t.execute()))
//                .collect(Collectors.toList());
//                new ArrayList<>();
        for (ScanTask task : scanner.scan()) {
            for (ScanTask.BatchIterator it = task.execute(); it.hasNext(); ) {
                ArrowRecordBatch batch = it.next();
                batches.add(batch);
//                batch.getNodes()
            }
        }


        List<Field> fields = factory.inspect().getFields();
        List<FieldVector> vectors = fields.stream().map(field -> field.createVector(allocator)).collect(Collectors.toList());
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        VectorLoader loader = new VectorLoader(root);
        loader.load(batches.get(0));


        Object[][] items = new Object[root.getRowCount()][fields.size()];
        System.out.println(root.getRowCount());
        System.out.println(fields.size());
        for (int i = 0; i < vectors.size(); i++) {
            for (int j = 0; j < root.getRowCount(); j++) {
                items[j][i] = vectors.get(i).getObject(j);
            }
        }

//        VectorLoader loader =

//        int n = batches.get(0).getLength();
//        for (int i = 0; i < n; i++) {
//            for (ArrowRecordBatch batch : batches) {
//                for (Object o : batch) {
//
//                }
//            }
//        }

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
