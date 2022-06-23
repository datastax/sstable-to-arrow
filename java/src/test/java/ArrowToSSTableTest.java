import com.datastax.sstablearrow.ArrowToSSTable;
import com.datastax.sstablearrow.Util;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ArrowToSSTableTest {

    static String keyspace = "testing", table = "alltypes";

    S3Client s3 = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
            .region(Region.US_WEST_2)
            .build();

    String BUCKET_NAME = "astra-byob-dev";

    @BeforeAll
    void init() {
        Util.init();
    }

    @Test
    void testAllTypes() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot allTypesArrow = getArrowTableAllTypes(allocator);
            TableMetadata cassandraTable = createMatchingTable(allTypesArrow);
            Map<ColumnIdentifier, FieldVector> mapping = ArrowToSSTable.alignSchemas(allTypesArrow, cassandraTable);
            Path outputPath = Files.createTempFile("arrow-output-", ".arrow");
            System.out.println("writing output to " + outputPath);
            FileChannel out = FileChannel.open(outputPath, StandardOpenOption.WRITE);
            ArrowFileWriter writer = new ArrowFileWriter(allTypesArrow, null, out);
            writer.start();
            writer.writeBatch();
            writer.end();
            Path sstablePath = Files.createTempDirectory("sstable-output-");
            System.out.println("writing SSTable to " + sstablePath);
            ArrowToSSTable.writeCqlSSTable(cassandraTable, mapping, sstablePath, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    TableMetadata createMatchingTable(VectorSchemaRoot root) {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table);
        builder.addPartitionKeyColumn("key", UTF8Type.instance);
        root.getFieldVectors()
                .forEach(vector -> {
                    if (!vector.getName()
                            .equals("key"))
                        builder.addRegularColumn(vector.getName(), ArrowToSSTable.getMatchingCassandraType(vector.getMinorType()));
                });
        return builder.build();
    }

    /**
     * Creates an Arrow table (VectorSchemaRoot) with all supported import types, as well as a key vector.
     * Note that counters are not included since we can't import them into a Cassandra table.
     */
    VectorSchemaRoot getArrowTableAllTypes(BufferAllocator allocator) {
        Map<Types.MinorType, FieldVector> vectors = Arrays.stream(Types.MinorType.values())
                .filter(value -> {
                    switch (value) {
                        case NULL:
                        case STRUCT:
                        case DATEDAY:
                        case DATEMILLI:
                        case TIMESEC:
                        case TIMEMILLI:
                        case TIMEMICRO:
                        case TIMENANO:
                        case TIMESTAMPSEC:
                        case TIMESTAMPMILLI:
                        case TIMESTAMPMICRO:
                        case TIMESTAMPNANO:
                        case INTERVALDAY:
                        case DURATION:
                        case INTERVALYEAR:
                        case LIST:
                        case LARGELIST:
                        case FIXED_SIZE_LIST:
                        case UNION:
                        case DENSEUNION:
                        case MAP:
                        case TIMESTAMPSECTZ:
                        case TIMESTAMPMILLITZ:
                        case TIMESTAMPMICROTZ:
                        case TIMESTAMPNANOTZ:
                        case EXTENSIONTYPE:
                        case LARGEVARBINARY:
                        case VARBINARY:
                        case FIXEDSIZEBINARY:
                            return false;
                        default:
                            return true;
                    }
                })
                .map(value -> {
                    FieldType fieldType;
                    if (value == Types.MinorType.DURATION)
                        fieldType = FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND));
                    else if (value == Types.MinorType.DECIMAL)
                        fieldType = FieldType.nullable(new ArrowType.Decimal(16, 15, 128));
                    else if (value == Types.MinorType.DECIMAL256)
                        fieldType = FieldType.nullable(new ArrowType.Decimal(69, 15, 256));
                    else if (value == Types.MinorType.FIXEDSIZEBINARY)
                        fieldType = FieldType.nullable(new ArrowType.FixedSizeBinary("chicken nugget".getBytes(StandardCharsets.UTF_8).length));
                    else if (value == Types.MinorType.TIMESTAMPSECTZ)
                        fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "EST"));
                    else if (value == Types.MinorType.TIMESTAMPMILLITZ)
                        fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "EST"));
                    else if (value == Types.MinorType.TIMESTAMPMICROTZ)
                        fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "EST"));
                    else if (value == Types.MinorType.TIMESTAMPNANOTZ)
                        fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "EST"));
                    else fieldType = FieldType.nullable(value.getType());
                    return value.getNewVector(value.toString() + "_val", fieldType, allocator, null);
                })
                .collect(Collectors.toMap(vector -> vector.getMinorType(), vector -> vector));

        ((TinyIntVector) vectors.get(Types.MinorType.TINYINT)).setSafe(0, 42);
        ((SmallIntVector) vectors.get(Types.MinorType.SMALLINT)).setSafe(0, Short.MAX_VALUE);
        ((IntVector) vectors.get(Types.MinorType.INT)).setSafe(0, Integer.MAX_VALUE);
        ((BigIntVector) vectors.get(Types.MinorType.BIGINT)).setSafe(0, Long.MAX_VALUE);
//        ((DateDayVector) vectors.get(Types.MinorType.DATEDAY)).setSafe(0, 1 << 31);
//        ((DateMilliVector) vectors.get(Types.MinorType.DATEMILLI)).setSafe(0, 3000);
//        ((TimeSecVector) vectors.get(Types.MinorType.TIMESEC)).setSafe(0, 60 * 6);
//        ((TimeMilliVector) vectors.get(Types.MinorType.TIMEMILLI)).setSafe(0, 1000 * 60 * 6);
//        ((TimeMicroVector) vectors.get(Types.MinorType.TIMEMICRO)).setSafe(0, 1000 * 1000 * 6);
//        ((TimeNanoVector) vectors.get(Types.MinorType.TIMENANO)).setSafe(0, (long) (1e9 * 6));
//        ((TimeStampSecVector) vectors.get(Types.MinorType.TIMESTAMPSEC)).setSafe(0, 5);
//        ((TimeStampMilliVector) vectors.get(Types.MinorType.TIMESTAMPMILLI)).setSafe(0, 1000 * 5);
//        ((TimeStampMicroVector) vectors.get(Types.MinorType.TIMESTAMPMICRO)).setSafe(0, 1000 * 1000 * 5);
//        ((TimeStampNanoVector) vectors.get(Types.MinorType.TIMESTAMPNANO)).setSafe(0, (long) (1e9 * 5));
//        ((IntervalDayVector) vectors.get(Types.MinorType.INTERVALDAY)).setSafe(0, 10, 1000 * 60 * 5);
//        ((DurationVector) vectors.get(Types.MinorType.DURATION)).setSafe(0, 1000 * 60 * 5); // 5 minutes
//        ((IntervalYearVector) vectors.get(Types.MinorType.INTERVALYEAR)).setSafe(0, 5);
        ((Float4Vector) vectors.get(Types.MinorType.FLOAT4)).setSafe(0, 6.2831853072f);
        ((Float8Vector) vectors.get(Types.MinorType.FLOAT8)).setSafe(0, Math.PI);
        ((BitVector) vectors.get(Types.MinorType.BIT)).setSafe(0, 1);
        ((VarCharVector) vectors.get(Types.MinorType.VARCHAR)).setSafe(0, "hello world".getBytes(StandardCharsets.UTF_8));
        ((LargeVarCharVector) vectors.get(Types.MinorType.LARGEVARCHAR)).setSafe(0, "lorem ipsum dolor sit amet".getBytes(StandardCharsets.UTF_8));
//        ((LargeVarBinaryVector) vectors.get(Types.MinorType.LARGEVARBINARY)).setSafe(0, "foo bar baz".getBytes(StandardCharsets.UTF_8));
//        ((VarBinaryVector) vectors.get(Types.MinorType.VARBINARY)).setSafe(0, "sus".getBytes(StandardCharsets.UTF_8));
        ((DecimalVector) vectors.get(Types.MinorType.DECIMAL)).setSafe(0, BigDecimal.valueOf(Math.E)
                .setScale(15));
        ((Decimal256Vector) vectors.get(Types.MinorType.DECIMAL256)).setSafe(0, new BigDecimal("808017424794512875886459904961710757005754368000000000").setScale(15));
//        ((FixedSizeBinaryVector) vectors.get(Types.MinorType.FIXEDSIZEBINARY)).setSafe(0, "chicken nugget".getBytes(StandardCharsets.UTF_8));
        ((UInt1Vector) vectors.get(Types.MinorType.UINT1)).setSafe(0, (1 << 8) - 1);
        ((UInt2Vector) vectors.get(Types.MinorType.UINT2)).setSafe(0, (1 << 16) - 1);
        ((UInt4Vector) vectors.get(Types.MinorType.UINT4)).setSafe(0, (1 << 32) - 1);
        ((UInt8Vector) vectors.get(Types.MinorType.UINT8)).setSafe(0, (1 << 64) - 1);
//        ((TimeStampSecTZVector) vectors.get(Types.MinorType.TIMESTAMPSECTZ)).setSafe(0, 30);
//        ((TimeStampMilliTZVector) vectors.get(Types.MinorType.TIMESTAMPMILLITZ)).setSafe(0, 30 * 1000);
//        ((TimeStampMicroTZVector) vectors.get(Types.MinorType.TIMESTAMPMICROTZ)).setSafe(0, 30 * 1000 * 1000);
//        ((TimeStampNanoTZVector) vectors.get(Types.MinorType.TIMESTAMPNANOTZ)).setSafe(0, (long) (30 * 1e9));

        vectors.values()
                .forEach(vector -> vector.setValueCount(1));

        VarCharVector keyVector = new VarCharVector("key", allocator);
        keyVector.setSafe(0, "bananas".getBytes(StandardCharsets.UTF_8));
        keyVector.setValueCount(1);

        List<Field> fields = new ArrayList<>();
        List<FieldVector> fieldVectors = new ArrayList<>();
        fields.add(keyVector.getField());
        fieldVectors.add(keyVector);

        vectors.forEach((field, vec) -> {
            fields.add(vec.getField());
            fieldVectors.add(vec);
        });

        return new VectorSchemaRoot(fields, fieldVectors);
    }

    @Test
    void getInsertStatement() {
//        ArrowToSSTable.
    }

    @Test
    void downloadAllParquetFiles() {
        List<Path> paths = null;
        try {
            paths = ArrowToSSTable.downloadAllParquetFiles(s3, BUCKET_NAME);
        } catch (IOException e) {
            Assert.fail("Error downloading parquet files: " + e.getMessage());
        }
        Path dataDir = Paths.get(System.getProperty("user.dir") + File.separator + "data" + File.separator + keyspace + File.separator + table);

        paths.forEach(path -> {
            try {
                String uri = path.toUri()
                        .toString();
                System.out.println("reading from " + uri);
                VectorSchemaRoot root = ArrowToSSTable.readParquetFile(uri);
//                Map<ColumnIdentifier, FieldVector> aligned = ArrowToSSTable.alignSchemas(root, allNativeTypesTable);
                System.out.println("writing to SSTable");
//                ArrowToSSTable.writeCqlSSTable(allNativeTypesTable, aligned, dataDir);
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