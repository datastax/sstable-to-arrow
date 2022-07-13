import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.datastax.cndb.bulkimport.BulkImporterApplication;
import com.datastax.sstablearrow.ArrowToSSTable;
import com.datastax.sstablearrow.ArrowUtils;
import com.datastax.sstablearrow.ParquetReaderUtils;
import com.datastax.sstablearrow.SSTableWriterUtils;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrowToSSTableTest
{

    public static final String TEST_KEYSPACE = "testing", TEST_TABLE = "alltypes";

    public static final S3Client s3 = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("alex.cai"))
            .region(Region.US_WEST_2)
            .build();

    public static final String BUCKET_NAME = "astra-byob-dev";

    @BeforeAll
    public static void init()
    {
        BulkImporterApplication.init();
    }

    @Test
    public void testAlignSchemas()
    {
        VectorSchemaRoot table = getArrowTableWithAllTypes();
        TableMetadata metadata = TableMetadata.builder(TEST_KEYSPACE, TEST_TABLE)
                .addPartitionKeyColumn("key", UTF8Type.instance)
                .addRegularColumn("UINT1_val", ShortType.instance)
                .build();
        try
        {
            ArrowToSSTable.alignSchemas(table, metadata);
        }
        catch (Exception e)
        {
            Assertions.fail(e.getMessage());
        }
    }

    /**
     * Creates an Arrow table (VectorSchemaRoot) with all supported Arrow MinorTypes, as well as a key vector.
     * Note that counters are not included since we can't import them into a Cassandra table.
     */
    public VectorSchemaRoot getArrowTableWithAllTypes()
    {
        Map<Types.MinorType, FieldVector> vectors = Arrays.stream(Types.MinorType.values())
                .filter(minorType -> {
                    try
                    {
                        ArrowToSSTable.getMatchingCassandraType(minorType);
                        return true;
                    }
                    catch (Exception e)
                    {
                        return false;
                    }
                })
                .map(minorType -> {
                    FieldType fieldType;
                    // handle parameterized minor types. These only need to be done manually if the minor type does not
                    // already have an Arrow type
                    switch (minorType)
                    {
                        case DURATION:
                            fieldType = FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND));
                            break;
                        case DECIMAL:
                            fieldType = FieldType.nullable(new ArrowType.Decimal(16, 15, 128));
                            break;
                        case DECIMAL256:
                            fieldType = FieldType.nullable(new ArrowType.Decimal(69, 15, 256));
                            break;
                        case FIXEDSIZEBINARY:
                            fieldType = FieldType.nullable(new ArrowType.FixedSizeBinary("chicken nugget".getBytes(StandardCharsets.UTF_8).length));
                            break;
                        case TIMESTAMPSECTZ:
                            fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "EST"));
                            break;
                        case TIMESTAMPMILLITZ:
                            fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "EST"));
                            break;
                        case TIMESTAMPMICROTZ:
                            fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "EST"));
                            break;
                        case TIMESTAMPNANOTZ:
                            fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "EST"));
                            break;
                        default:
                            fieldType = FieldType.nullable(minorType.getType());
                    }
                    return minorType.getNewVector(minorType + "_val", fieldType, ArrowUtils.ALLOCATOR, null);
                })
                .collect(Collectors.toMap(vector -> vector.getMinorType(), vector -> vector));

        ((TinyIntVector) vectors.get(Types.MinorType.TINYINT)).setSafe(0, 42);
        ((SmallIntVector) vectors.get(Types.MinorType.SMALLINT)).setSafe(0, Short.MAX_VALUE);
        ((IntVector) vectors.get(Types.MinorType.INT)).setSafe(0, Integer.MAX_VALUE);
        ((BigIntVector) vectors.get(Types.MinorType.BIGINT)).setSafe(0, Long.MAX_VALUE);

        ((DateDayVector) vectors.get(Types.MinorType.DATEDAY)).setSafe(0, 1 << 31);
        ((DateMilliVector) vectors.get(Types.MinorType.DATEMILLI)).setSafe(0, 3000);

        // should all be the same time, 6 minutes past midnight
        ((TimeSecVector) vectors.get(Types.MinorType.TIMESEC)).setSafe(0, 60 * 6);
        ((TimeMilliVector) vectors.get(Types.MinorType.TIMEMILLI)).setSafe(0, 1000 * 60 * 6);
        ((TimeMicroVector) vectors.get(Types.MinorType.TIMEMICRO)).setSafe(0, (long) (1e6 * 60 * 6));
        ((TimeNanoVector) vectors.get(Types.MinorType.TIMENANO)).setSafe(0, (long) (1e9 * 60 * 6));

        // all the same timestamp, 5 seconds past epoch
        ((TimeStampSecVector) vectors.get(Types.MinorType.TIMESTAMPSEC)).setSafe(0, 5);
        ((TimeStampMilliVector) vectors.get(Types.MinorType.TIMESTAMPMILLI)).setSafe(0, 1000 * 5);
        ((TimeStampMicroVector) vectors.get(Types.MinorType.TIMESTAMPMICRO)).setSafe(0, (long) (1e6 * 5));
        ((TimeStampNanoVector) vectors.get(Types.MinorType.TIMESTAMPNANO)).setSafe(0, (long) (1e9 * 5));

        // intervals
        ((IntervalDayVector) vectors.get(Types.MinorType.INTERVALDAY)).setSafe(0, 10, 1000 * 60 * 5);
        ((DurationVector) vectors.get(Types.MinorType.DURATION)).setSafe(0, 1000 * 60 * 5); // 5 minutes
        ((IntervalYearVector) vectors.get(Types.MinorType.INTERVALYEAR)).setSafe(0, 5);

        // floats and doubles
        ((Float4Vector) vectors.get(Types.MinorType.FLOAT4)).setSafe(0, 6.2831853072f);
        ((Float8Vector) vectors.get(Types.MinorType.FLOAT8)).setSafe(0, Math.PI);

        // bits and strings
        ((BitVector) vectors.get(Types.MinorType.BIT)).setSafe(0, 1);
        ((VarCharVector) vectors.get(Types.MinorType.VARCHAR)).setSafe(0, "hello world".getBytes(StandardCharsets.UTF_8));
        ((LargeVarCharVector) vectors.get(Types.MinorType.LARGEVARCHAR)).setSafe(0, "lorem ipsum dolor sit amet".getBytes(StandardCharsets.UTF_8));
        ((LargeVarBinaryVector) vectors.get(Types.MinorType.LARGEVARBINARY)).setSafe(0, "foo bar baz".getBytes(StandardCharsets.UTF_8));
        ((VarBinaryVector) vectors.get(Types.MinorType.VARBINARY)).setSafe(0, "'Twas brillig, and the slithy toves".getBytes(StandardCharsets.UTF_8));

        // decimals
        ((DecimalVector) vectors.get(Types.MinorType.DECIMAL)).setSafe(0, BigDecimal.valueOf(Math.E).setScale(15));
        ((Decimal256Vector) vectors.get(Types.MinorType.DECIMAL256)).setSafe(0, new BigDecimal("808017424794512875886459904961710757005754368000000000").setScale(15));

        ((FixedSizeBinaryVector) vectors.get(Types.MinorType.FIXEDSIZEBINARY)).setSafe(0, "chicken nugget".getBytes(StandardCharsets.UTF_8));

        // unsigned ints
        ((UInt1Vector) vectors.get(Types.MinorType.UINT1)).setSafe(0, (1 << 8) - 1);
        ((UInt2Vector) vectors.get(Types.MinorType.UINT2)).setSafe(0, (1 << 16) - 1);
        ((UInt4Vector) vectors.get(Types.MinorType.UINT4)).setSafe(0, (1 << 32) - 1);
        ((UInt8Vector) vectors.get(Types.MinorType.UINT8)).setSafe(0, (1 << 64) - 1);

        // timestamp with timezone
        ((TimeStampSecTZVector) vectors.get(Types.MinorType.TIMESTAMPSECTZ)).setSafe(0, 30);
        ((TimeStampMilliTZVector) vectors.get(Types.MinorType.TIMESTAMPMILLITZ)).setSafe(0, 30 * 1000);
        ((TimeStampMicroTZVector) vectors.get(Types.MinorType.TIMESTAMPMICROTZ)).setSafe(0, 30 * 1000 * 1000);
        ((TimeStampNanoTZVector) vectors.get(Types.MinorType.TIMESTAMPNANOTZ)).setSafe(0, (long) (30 * 1e9));

        vectors.values().forEach(vector -> vector.setValueCount(1));

        VarCharVector keyVector = new VarCharVector("key", ArrowUtils.ALLOCATOR);
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
    public void testGetArrowTable()
    {
        VectorSchemaRoot table = getArrowTableWithAllTypes();
        assertEquals(1, table.getRowCount());
        assertEquals(table.getVector("TIMESTAMPMICRO_val").getObject(0), table.getVector("TIMESTAMPMILLI_val").getObject(0));
        assertEquals(table.getVector("TIMESTAMPNANO_val").getObject(0), table.getVector("TIMESTAMPMILLI_val").getObject(0));
        assertEquals(table.getVector("TIMESTAMPNANO_val").getObject(0), table.getVector("TIMESTAMPSEC_val").getObject(0));
    }

    @Test
    public void testAllTypes() throws IOException, ExecutionException, InterruptedException
    {
        VectorSchemaRoot allTypesArrow = getArrowTableWithAllTypes();
        TableMetadata cassandraTable = ArrowToSSTable.createMatchingTable(allTypesArrow, TEST_KEYSPACE, TEST_TABLE, false);
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
        SSTableWriterUtils.writeCqlSSTable(cassandraTable, mapping, sstablePath, true);
    }

    @Test
    void testGetInsertStatement()
    {
        TableMetadata schema = TableMetadata.builder("foo", "bar")
                .addPartitionKeyColumn("key", Int32Type.instance)
                .addRegularColumn("value", UTF8Type.instance)
                .build();
        String statement = SSTableWriterUtils.getInsertStatement(schema);
        assertEquals("INSERT INTO \"foo\".\"bar\" (key, value) VALUES (?, ?)", statement);
    }

    @Test
    public void testDownloadAllParquetFiles()
    {
        List<Path> paths = null;
        try
        {
            List<S3Object> parquetObjects = ArrowToSSTable.listParquetFiles(s3, BUCKET_NAME);
            Path tempDir = Files.createTempDirectory("parquet-output-");
            paths = parquetObjects.stream()
                    .map(o -> ArrowToSSTable.downloadFile(s3, BUCKET_NAME, o, tempDir))
                    .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            Assertions.fail("Error downloading parquet files: " + e.getMessage());
        }

        // Path dataDir = Paths.get(System.getProperty("user.dir") + File.separator + "data" + File.separator + TEST_KEYSPACE + File.separator + TEST_TABLE);

        paths.forEach(path -> {
            try
            {
                String uri = path.toUri()
                        .toString();
                System.out.println("reading from " + uri);
                ParquetReaderUtils.read(uri, root -> System.out.println(root.contentToTSVString()));
                //                Map<ColumnIdentifier, FieldVector> aligned = ArrowToSSTable.alignSchemas(root, allNativeTypesTable);
                System.out.println("writing to SSTable");
                //                ArrowToSSTable.writeCqlSSTable(allNativeTypesTable, aligned, dataDir);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }
}