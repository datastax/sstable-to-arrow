package com.datastax.sstablearrow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static java.util.stream.StreamSupport.stream;

public class ArrowUtils
{

    public static final int PORT_NUMBER = 9143;
    public static final BufferAllocator ALLOCATOR = new RootAllocator();
    public static Map<String, Types.MinorType> typeMapping = new HashMap();
    //    A mapping from Cassandra column names to Arrow FieldVectors.
    private final Map<String, FieldVector> vectors = new HashMap();
    //    keeps track of the index to insert into each vector
    private int rowIndex = 0;
    private List<FilteredPartition> partitions;

    public ArrowUtils(List<FilteredPartition> partitions)
    {
        this.partitions = partitions;
    }

    /**
     * Should be called with the ColumnMetadata objects returned by row.columns()
     * TODO Several types are not yet implemented or tested
     *
     * @param column the column metadata from which to generate the FieldVector
     * @param rowCount the amount of elements to allocate memory for
     *
     * @return an Arrow FieldVector with memory allocated for `rowCount` elements
     */
    public static FieldVector createArrowVector(ColumnMetadata column, int rowCount)
    {
        // TODO unsure about the difference between this and toSchemaString
        String cql3Type = column.cellValueType()
                .asCQL3Type()
                .toString();
        String colName = column.name.toString();
        Types.MinorType matchingType = typeMapping.get(cql3Type);
        FieldVector vec = matchingType.getNewVector(colName, FieldType.nullable(matchingType.getType()), ALLOCATOR, null);
        if (vec instanceof VariableWidthVector) ((VariableWidthVector) vec).allocateNew(rowCount);
        else if (vec instanceof FixedWidthVector) ((FixedWidthVector) vec).allocateNew(rowCount);
        else throw new IllegalArgumentException("Unsupported vector type: " + vec.getClass().getName());
        return vec;
    }

    /**
     * Send the filled Arrow FieldVectors as a RecordBatch across a network socket specified by `PORT_NUMBER`
     */
    private static void transferArrowIpc(VectorSchemaRoot root)
    {
        System.out.println("Listening for socket on port " + PORT_NUMBER);

        //        Send data directly on connection
        try (
                ServerSocket serverSocket = new ServerSocket(PORT_NUMBER);
                Socket clientSocket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
        )
        {
            System.out.println("connected to socket from address " + clientSocket.getInetAddress());

            // wait for signal from client
            String line = in.readLine();
            System.out.println("Received " + line);

            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(clientSocket.getOutputStream()));

            writer.start();
            writer.writeBatch();
            writer.end();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static Types.MinorType matchingType(AbstractType cassandraType)
    {
        return typeMapping.get(cassandraType.asCQL3Type()
                .toString());
    }

    public static boolean validType(AbstractType cassandraType, Types.MinorType arrowType)
    {
        return matchingType(cassandraType) == arrowType;
    }

    /**
     * Take the item in the Arrow FieldVector at the given index,
     * and convert it to a general Java Object to be passed to CQLSSTableWriter.
     *
     * see <a href="https://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/DataType.Name.html#asJavaClass--">asJavaClass</a>
     *
     * @param vector the FieldVector to read from
     * @param index the index of the item to read
     *
     * @return the Java Object representation of the item at the given index
     */
    public static Object toJavaClass(FieldVector vector, int index)
    {
        switch (vector.getMinorType())
        {
            case NULL:
                break;
            case STRUCT:
                break;
            case DATEDAY:
                Integer day = ((DateDayVector) vector).getObject(index);
                return new Date(day.longValue() * 24 * 60 * 60 * 1000);
            case DATEMILLI:
                Instant instant = ((DateMilliVector) vector).getObject(index)
                        .toInstant(ZoneOffset.UTC);
                return Date.from(instant);
            case TIMESEC:
                Integer sec = ((TimeSecVector) vector).getObject(index);
                return sec.longValue() * 1e9;
            case TIMEMILLI:
                Instant milliIinstant = ((TimeMilliVector) vector).getObject(index)
                        .toInstant(ZoneOffset.UTC);
                return Date.from(milliIinstant)
                        .getTime();
            case TIMEMICRO:
                Long val = ((TimeMicroVector) vector).getObject(index);
                return val * 1000;
            case TIMENANO:
                return ((TimeNanoVector) vector).getObject(index);
            case TIMESTAMPSEC:
                break;
            case TIMESTAMPMILLI:
                break;
            case TIMESTAMPMICRO:
                break;
            case TIMESTAMPNANO:
                break;
            case INTERVALDAY:
                break;
            case DURATION:
                break;
            case INTERVALYEAR:
                break;
            case FLOAT4:
                break;
            case FLOAT8:
                break;
            case BIT:
                break;
            case VARCHAR:
                return ((VarCharVector) vector).getObject(index)
                        .toString();
            case LARGEVARCHAR:
                return ((LargeVarCharVector) vector).getObject(index)
                        .toString();
            case LARGEVARBINARY:
                break;
            case VARBINARY:
                break;
            case DECIMAL:
                break;
            case DECIMAL256:
                break;
            case FIXEDSIZEBINARY:
                break;
            case UINT1:
                break;
            case UINT2:
                break;
            case UINT4:
                break;
            case UINT8:
            case LIST:
                break;
            case LARGELIST:
                break;
            case FIXED_SIZE_LIST:
                break;
            case UNION:
                break;
            case DENSEUNION:
                break;
            case MAP:
                break;
            case TIMESTAMPSECTZ:
                break;
            case TIMESTAMPMILLITZ:
                break;
            case TIMESTAMPMICROTZ:
                break;
            case TIMESTAMPNANOTZ:
                break;
            case EXTENSIONTYPE:
                break;
        }

        //        "this
        //        throw NotImplementedException();

        return vector.getObject(index);
    }

    public void process()
    {
        long startTime = System.nanoTime();
        int rowCount = partitions.stream()
                .mapToInt(partition -> partition.rowCount())
                .sum();

        // Arrow Vectors:
        // creation > allocation > mutation > set value count > access > clear
        // see http://arrow.apache.org/docs/java/vector.html

        //        For some reason, partition.metadata.columns() doesn't actually give all the columns and also has
        //        incorrect types
        //        so instead we get the primary key columns (partition key and clustering)...
        for (ColumnMetadata col : partitions.get(0)
                .metadata()
                .primaryKeyColumns())
        {
            FieldVector vec = createArrowVector(col, rowCount);
            vec.allocateNew();
            vectors.put(col.name.toString(), vec);
        }
        //        and then the columns in each row, which only contain the "actual values" (no primary key)
        for (ColumnMetadata col : partitions.get(0)
                .lastRow()
                .columns())
        {
            FieldVector vec = createArrowVector(col, rowCount);
            vec.allocateNew();
            vectors.put(col.name.toString(), vec);
        }

        //
        for (FilteredPartition partition : partitions)
        {
            processPartition(partition);
        }

        for (Map.Entry<String, FieldVector> entry : vectors.entrySet())
        {
            //            System.out.println("===== VECTOR " + entry.getKey() + " =====");
            ValueVector vec = entry.getValue();
            vec.setValueCount(rowCount);

            //            for (int i = 0; i < vec.getValueCount(); i++) {
            //                System.out.println(vec.getObject(i));
            //            }
        }

        List<FieldVector> arrays = new ArrayList(vectors.values());
        List<Field> fields = arrays.stream()
                .map(vec -> vec.getField())
                .collect(Collectors.toList());

        VectorSchemaRoot root = new VectorSchemaRoot(fields, arrays);
        //        transferArrowIpc(root);

        vectors.values()
                .forEach(vector -> vector.close());
        root.close();
        System.out.println("[PROFILE] arrow transfer: " + (System.nanoTime() - startTime));
    }

    /**
     * Appends the cells in each row of `partition` into the corresponding Arrow ValueVector
     */
    private void processPartition(FilteredPartition partition)
    {
        TableMetadata metadata = partition.metadata();

        for (Row row : partition)
        {
            //            insert partition key (maybe this can be optimized, since will be the same for each row in the partition)
            List<AbstractType<?>> partitionKeyColumns = new ArrayList<>();
            for (ColumnMetadata col : metadata.partitionKeyColumns())
            {
                partitionKeyColumns.add(col.type);
            }
            CompositeType partitionKeyType = CompositeType.getInstance(partitionKeyColumns);
            ByteBuffer[] splitted = partitionKeyType.split(partition.partitionKey()
                    .getKey());

            for (int i = 0; i < splitted.length; ++i)
            {
                ColumnMetadata col = metadata.partitionKeyColumns()
                        .get(i);
                appendToVector(col, rowIndex, col.type.compose(splitted[i]));
            }

            //            insert clustering
            for (int i = 0; i < metadata.clusteringColumns()
                    .size(); i++)
            {
                ColumnMetadata col = metadata.clusteringColumns()
                        .get(i);
                Object clustering = col.type.compose(row.clustering()
                        .get(i));
                appendToVector(col, rowIndex, clustering);
            }

            //            insert other cells
            for (ColumnMetadata col : row.columns())
            {
                Cell cell = row.getCell(col);
                Object value = col.cellValueType()
                        .compose(cell.value());
                appendToVector(col, rowIndex, value);
            }

            //            increment global row index
            rowIndex++;
        }
    }

    /**
     * Assumes that the proper type of value is passed for each type of vector
     *
     * @param col the column, corresponding to a FieldVector, to insert the value into
     * @param rowIndex the index to insert the value at
     * @param value the value to insert
     */
    private void appendToVector(ColumnMetadata col, int rowIndex, Object value)
    {
        //        unsure about the difference between this and toSchemaString
        //        switching on CQL3/Cassandra/SSTable type and returning an Arrow ValueVector

        FieldVector vec = vectors.get(col.name.toString());
        String cql3Type = col.type.asCQL3Type().toString();
        switch (cql3Type)
        {
            case "ascii":
            case "varchar":
            case "inet":
            case "time":
            case "timeuuid": // potential change
            case "varint": // potential change
                // See vector creation section for complications
                ((VarCharVector) vec).setSafe(rowIndex, ((String) value).getBytes(StandardCharsets.UTF_8));
                break;
            case "uuid": // potential change
                UUID uuid = ((UUID) value);
                ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
                bb.putLong(uuid.getMostSignificantBits());
                bb.putLong(uuid.getLeastSignificantBits());
                ((FixedSizeBinaryVector) vec).setSafe(rowIndex, bb.array());
                break;
            case "bigint":
            case "counter":
                //                8 bytes = 36 bits
                ((BigIntVector) vec).setSafe(rowIndex, (Long) value);
                break;
            case "blob":
            case "frozen":
                //                Cassandra treats it as a blob, so this should work
                //                maybe VarBinaryVector? FixedSizeBinaryVector?
                ((LargeVarBinaryVector) vec).setSafe(rowIndex, (byte[]) value);
                break;
            case "boolean":
                ((BitVector) vec).setSafe(rowIndex, (Integer) value);
                break;
            case "date":
                ((DateDayVector) vec).setSafe(rowIndex, (Integer) value);
                break;
            case "decimal":
            case "double":
                //                TODO not sure about how to determine precision and scale for decimals yet
                //                currently just using floats
                ((Float8Vector) vec).setSafe(rowIndex, (Double) value);
                break;
            case "float":
                ((Float4Vector) vec).setSafe(rowIndex, (Float) value);
                break;
            case "int":
                ((IntVector) vec).setSafe(rowIndex, (Integer) value);
                break;
            case "list":
            case "map":
            case "set":
            case "tuple":
                //                TODO List/Set/Map/Tuple writing
                System.out.println("LIST WRITING NOT YET SUPPORTED");
                //                UnionListWriter writer = ((ListVector) vec).getWriter();
                //                writer.startList();
                //                writer.setPosition(i);
                //                for (Object obj : (Iterable) value) {
                //                    writer.write ?
                //                }
                //                writer.setValueCount(something);
                //                writer.endList();
                //                break;
                break;
            case "timestamp":
                ((TimeStampMilliVector) vec).setSafe(rowIndex, ((Date) value).toInstant().toEpochMilli());
                break;

            case "smallint":
                //                2 bytes
                ((SmallIntVector) vec).setSafe(rowIndex, (Short) value);
                break;
            case "text":
                ((LargeVarCharVector) vec).setSafe(rowIndex, ((String) value).getBytes(StandardCharsets.UTF_8));
                break;
            case "tinyint":
                ((TinyIntVector) vec).setSafe(rowIndex, (Byte) value);
                break;
            default:
                System.out.println("ERROR: CQL3 type not implemented: " + cql3Type);
        }
    }

    static
    {
        typeMapping.put("ascii", Types.MinorType.VARCHAR);
        typeMapping.put("bigint", Types.MinorType.BIGINT);
        typeMapping.put("blob", Types.MinorType.LARGEVARBINARY);
        typeMapping.put("boolean", Types.MinorType.BIT);
        typeMapping.put("counter", Types.MinorType.BIGINT);
        typeMapping.put("date", Types.MinorType.DATEDAY); // double check
        typeMapping.put("decimal", Types.MinorType.DECIMAL); // double check
        typeMapping.put("double", Types.MinorType.FLOAT8);
        typeMapping.put("duration", Types.MinorType.DURATION); // double check
        typeMapping.put("float", Types.MinorType.FLOAT4);
        typeMapping.put("inet", Types.MinorType.VARCHAR); // treat as string
        typeMapping.put("int", Types.MinorType.INT);
        typeMapping.put("smallint", Types.MinorType.SMALLINT);
        typeMapping.put("text", Types.MinorType.LARGEVARCHAR);
        typeMapping.put("time", Types.MinorType.TIMENANO);
        typeMapping.put("timestamp", Types.MinorType.TIMESTAMPMILLI);
        typeMapping.put("timeuuid", Types.MinorType.FIXEDSIZEBINARY);
        typeMapping.put("tinyint", Types.MinorType.TINYINT);
        typeMapping.put("uuid", Types.MinorType.FIXEDSIZEBINARY);
        typeMapping.put("varint", Types.MinorType.BIGINT);
    }
}
