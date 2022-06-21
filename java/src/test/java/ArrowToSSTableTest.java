import com.datastax.sstablearrow.ArrowToSSTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ArrowToSSTableTest {

    static String keyspace = "testing", table = "alltypes";

    //    We exclude counters since they can't be imported into Cassandra.
    TableMetadata allNativeTypesTable = TableMetadata.builder(keyspace, table)
            .addPartitionKeyColumn("key", UTF8Type.instance)
            .addRegularColumn("ascii_val", AsciiType.instance)
            .addRegularColumn("bigint_val", LongType.instance)
            .addRegularColumn("blob_val", BytesType.instance)
            .addRegularColumn("boolean_val", BooleanType.instance)
//            .addRegularColumn("counter_val", CounterColumnType.instance)
            .addRegularColumn("date_val", SimpleDateType.instance)
            .addRegularColumn("decimal_val", DecimalType.instance)
            .addRegularColumn("double_val", DoubleType.instance)
            .addRegularColumn("duration_val", DurationType.instance)
            .addRegularColumn("empty_val", EmptyType.instance)
            .addRegularColumn("float_val", FloatType.instance)
            .addRegularColumn("inet_val", InetAddressType.instance)
            .addRegularColumn("int_val", Int32Type.instance)
            .addRegularColumn("smallint_val", ShortType.instance)
            .addRegularColumn("text_val", UTF8Type.instance)
            .addRegularColumn("time_val", TimeType.instance)
            .addRegularColumn("timestamp_val", TimestampType.instance)
            .addRegularColumn("timeuuid_val", TimeUUIDType.instance)
            .addRegularColumn("tinyint_val", ByteType.instance)
            .addRegularColumn("uuid_val", UUIDType.instance)
            .addRegularColumn("varchar_val", UTF8Type.instance)
            .addRegularColumn("varint_val", IntegerType.instance)
            .build();

    S3Client s3 = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
            .region(Region.US_WEST_2)
            .build();

    String BUCKET_NAME = "astra-byob-dev";

    @BeforeAll
    static void init() throws IOException {
        System.setProperty("cassandra.system_view.only_local_and_peers_table", "true");
        System.setProperty("cassandra.config", "file:///" + System.getProperty("user.dir") + "/src/resources/cassandra.yaml");

        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @Test
    void fetchObjects() {
        System.out.println(ArrowToSSTable.fetchSchema(s3, BUCKET_NAME)
                .toString());
    }

    /**
     * Creates an Arrow table (VectorSchemaRoot) with all supported import types.
     * Note that counters are not included since we can't import them into a Cassandra table.
     */
    VectorSchemaRoot getArrowTableAllTypes() {
        try (BufferAllocator allocator = new RootAllocator()) {
            Map<Types.MinorType, ValueVector> vectors = Arrays.stream(Types.MinorType.values())
                    .filter(value -> value != Types.MinorType.NULL)
                    .map(value -> value.getNewVector(value.toString() + "_val", FieldType.nullable(value.getType()), allocator, null))
                    .collect(Collectors.toMap(vector -> vector.getMinorType(), vector -> vector));

            ((TinyIntVector) vectors.get(Types.MinorType.TINYINT)).setSafe(0, 42);
            ((SmallIntVector) vectors.get(Types.MinorType.SMALLINT)).setSafe(0, Short.MAX_VALUE);
            ((IntVector) vectors.get(Types.MinorType.INT)).setSafe(0, Integer.MAX_VALUE);
            ((BigIntVector) vectors.get(Types.MinorType.BIGINT)).setSafe(0, Long.MAX_VALUE);
            ((DateDayVector) vectors.get(Types.MinorType.DATEDAY)).setSafe(0, 1 << 31);
            ((DateMilliVector) vectors.get(Types.MinorType.DATEMILLI)).setSafe(0, 3000);
            ((TimeSecVector) vectors.get(Types.MinorType.TIMESEC)).setSafe(0, 60 * 6);
            ((TimeMilliVector) vectors.get(Types.MinorType.TIMEMILLI)).setSafe(0, 1000 * 60 * 6);
            ((TimeMicroVector) vectors.get(Types.MinorType.TIMEMICRO)).setSafe(0, 1000 * 1000 * 6);
            ((TimeNanoVector) vectors.get(Types.MinorType.TIMENANO)).setSafe(0, (long) (1e9 * 6));
            ((TimeStampSecVector) vectors.get(Types.MinorType.TIMESTAMPSEC)).setSafe(0, 5);
            ((TimeStampMilliVector) vectors.get(Types.MinorType.TIMESTAMPMILLI)).setSafe(0, 1000 * 5);
            ((TimeStampMicroVector) vectors.get(Types.MinorType.TIMESTAMPMICRO)).setSafe(0, 1000 * 1000 * 5);
            ((TimeStampNanoVector) vectors.get(Types.MinorType.TIMESTAMPNANO)).setSafe(0, (long) (1e9 * 5));
            ((IntervalDayVector) vectors.get(Types.MinorType.INTERVALDAY)).setSafe(0, 10, 1000 * 60 * 5);
            ((DurationVector) vectors.get(Types.MinorType.DURATION)).setSafe(0, 1000 * 60 * 5); // 5 minutes
            ((IntervalYearVector) vectors.get(Types.MinorType.INTERVALYEAR)).setSafe(0, 5);
            ((Float4Vector) vectors.get(Types.MinorType.FLOAT4)).setSafe(0, 6.2831853072f);
            ((Float8Vector) vectors.get(Types.MinorType.FLOAT8)).setSafe(0, Math.PI);
            ((BitVector) vectors.get(Types.MinorType.BIT)).setSafe(0, 1);
            ((VarCharVector) vectors.get(Types.MinorType.VARCHAR)).setSafe(0, "hello world".getBytes(StandardCharsets.UTF_8));
            ((LargeVarCharVector) vectors.get(Types.MinorType.LARGEVARCHAR)).setSafe(0, "lorem ipsum dolor sit amet".getBytes(StandardCharsets.UTF_8));
            ((LargeVarBinaryVector) vectors.get(Types.MinorType.LARGEVARBINARY)).setSafe(0, "foo bar baz".getBytes(StandardCharsets.UTF_8));
            ((VarBinaryVector) vectors.get(Types.MinorType.VARBINARY)).setSafe(0, "sus".getBytes(StandardCharsets.UTF_8));
            ((DecimalVector) vectors.get(Types.MinorType.DECIMAL)).setSafe(0, BigDecimal.valueOf(Math.E));
            ((Decimal256Vector) vectors.get(Types.MinorType.DECIMAL256)).setSafe(0, new BigDecimal("808017424794512875886459904961710757005754368000000000"));
            ((FixedSizeBinaryVector) vectors.get(Types.MinorType.FIXEDSIZEBINARY)).setSafe(0, "chicken nugget".getBytes(StandardCharsets.UTF_8));
            ((UInt1Vector) vectors.get(Types.MinorType.UINT1)).setSafe(0, (1 << 8) - 1);
            ((UInt2Vector) vectors.get(Types.MinorType.UINT2)).setSafe(0, (1 << 16) - 1);
            ((UInt4Vector) vectors.get(Types.MinorType.UINT4)).setSafe(0, (1 << 32) - 1);
            ((UInt8Vector) vectors.get(Types.MinorType.UINT8)).setSafe(0, (1 << 64) - 1);
            ((TimeStampSecTZVector) vectors.get(Types.MinorType.TIMESTAMPSECTZ)).setSafe(0, 30);
            ((TimeStampMilliTZVector) vectors.get(Types.MinorType.TIMESTAMPMILLITZ)).setSafe(0, 30 * 1000);
            ((TimeStampMicroTZVector) vectors.get(Types.MinorType.TIMESTAMPMICROTZ)).setSafe(0, 30 * 1000 * 1000);
            ((TimeStampNanoTZVector) vectors.get(Types.MinorType.TIMESTAMPNANOTZ)).setSafe(0, (long) (30 * 1e9));

//            collections
            NullableStructWriter writer = ((StructVector) vectors.get((Types.MinorType.STRUCT))).getWriter();
            writer.setPosition(0);
            writer.start();
            writer.writeDecimal(BigDecimal.TEN);
            writer.end();
            writer.setValueCount(1);

            vectors.values()
                    .forEach(vector -> vector.setValueCount(1));
        }

        return new VectorSchemaRoot(new ArrayList<>(), new ArrayList<>());
    }

    @Test
    void getInsertStatement() {
    }

    @Test
    void downloadAllParquetFiles() {
        List<Path> paths = ArrowToSSTable.downloadAllParquetFiles(s3, BUCKET_NAME);
        Path dataDir = Paths.get(System.getProperty("user.dir") + File.separator + "data" + File.separator + keyspace + File.separator + table);

        paths.forEach(path -> {
            try {
                String uri = path.toUri()
                        .toString();
                System.out.println("reading from " + uri);
                VectorSchemaRoot root = ArrowToSSTable.readParquetFile(uri);
                Map<ColumnIdentifier, FieldVector> aligned = ArrowToSSTable.alignSchemas(root, allNativeTypesTable);
                System.out.println("writing to SSTable");
                ArrowToSSTable.writeCqlSSTable(allNativeTypesTable, aligned, dataDir);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void readParquetFile() {
    }

    @Test
    void alignSchemas() {
    }

    @Test
    void writeCqlSSTable() {
    }
}