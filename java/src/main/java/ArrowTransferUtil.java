import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class ArrowTransferUtil {

    public static final int PORT_NUMBER = 9143;
    public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    //    keeps track of the index to insert into each vector
    private int rowIndex = 0;
    private final Map<String, FieldVector> vectors = new HashMap();
    private List<FilteredPartition> partitions;

    public ArrowTransferUtil(List<FilteredPartition> partitions) {
        this.partitions = partitions;
    }

    public void process() {
        long startTime = System.nanoTime();
        int rowCount = partitions.stream().mapToInt(partition -> partition.rowCount()).sum();

        // Arrow Vectors:
        // creation > allocation > mutation > set value count > access > clear
        // see http://arrow.apache.org/docs/java/vector.html

//        For some reason, partition.metadata.columns() doesn't actually give all the columns and also has
//        incorrect types
//        so instead we get the primary key columns (partition key and clustering)...
        for (ColumnDefinition col : partitions.get(0).metadata().primaryKeyColumns()) {
            FieldVector vec = createArrowVector(col, rowCount);
            vec.allocateNew();
            vectors.put(col.name.toString(), vec);
        }
//        and then the columns in each row, which only contain the "actual values" (no primary key)
        for (ColumnDefinition col : partitions.get(0).lastRow().columns()) {
            FieldVector vec = createArrowVector(col, rowCount);
            vec.allocateNew();
            vectors.put(col.name.toString(), vec);
        }

//
        for (FilteredPartition partition : partitions) {
            processPartition(partition);
        }

        for (Map.Entry<String, FieldVector> entry : vectors.entrySet()) {
//            System.out.println("===== VECTOR " + entry.getKey() + " =====");
            ValueVector vec = entry.getValue();
            vec.setValueCount(rowCount);

//            for (int i = 0; i < vec.getValueCount(); i++) {
//                System.out.println(vec.getObject(i));
//            }
        }


        List<FieldVector> arrays = new ArrayList(vectors.values());
        List<Field> fields = arrays.stream().map(vec -> vec.getField()).collect(Collectors.toList());

        VectorSchemaRoot root = new VectorSchemaRoot(fields, arrays);
//        transferArrowIpc(root);

        vectors.values().forEach(vector -> vector.close());
        root.close();
        System.out.println("[PROFILE] arrow transfer: " + (System.nanoTime() - startTime));
    }

    /**
     * Appends the cells in each row of `partition` into the corresponding Arrow ValueVector
     *
     * @param partition
     */
    private void processPartition(FilteredPartition partition) {
        CFMetaData metadata = partition.metadata();

        for (Row row : partition) {
//            insert partition key (maybe this can be optimized, since will be the same for each row in the partition)
            List<AbstractType<?>> partitionKeyColumns = new ArrayList<>();
            for (ColumnDefinition col : metadata.partitionKeyColumns()) {
                partitionKeyColumns.add(col.type);
            }
            CompositeType partitionKeyType = CompositeType.getInstance(partitionKeyColumns);
            ByteBuffer[] splitted = partitionKeyType.split(partition.partitionKey().getKey());

            for (int i = 0; i < splitted.length; ++i) {
                ColumnDefinition col = metadata.partitionKeyColumns().get(i);
                appendToVector(col, rowIndex, col.type.compose(splitted[i]));
            }

//            insert clustering
            for (int i = 0; i < metadata.clusteringColumns().size(); i++) {
                ColumnDefinition col = metadata.clusteringColumns().get(i);
                Object clustering = col.type.compose(row.clustering().get(i));
                appendToVector(col, rowIndex, clustering);
            }

//            insert other cells
            for (ColumnDefinition col : row.columns()) {
                Cell cell = row.getCell(col);
                Object value = col.cellValueType().compose(cell.value());
                appendToVector(col, rowIndex, value);
            }

//            increment global row index
            rowIndex++;
        }
    }

    /**
     * Should be called with the ColumnMetadata objects returned by row.columns()
     * TODO Several types are not yet implemented or tested
     *
     * @param column   the column metadata from which to generate the FieldVector
     * @param rowCount the amount of elements to allocate memory for
     * @return an Arrow FieldVector with memory allocated for `rowCount` elements
     */
    public static FieldVector createArrowVector(ColumnDefinition column, int rowCount) {
//        unsure about the difference between this and toSchemaString
//        switching on CQL3/Cassandra/SSTable type and returning an Arrow ValueVector
        String cql3Type = column.cellValueType().asCQL3Type().toString();
        String colName = column.name.toString();
        FieldVector vec;
        switch (cql3Type) {
            case "ascii":
            case "varchar":
                vec = new VarCharVector(colName, allocator);
                ((VarCharVector) vec).allocateNew(rowCount);
                break;
            case "bigint":
            case "counter":
//                8 bytes = 36 bits
                vec = new BigIntVector(colName, allocator);
                ((BigIntVector) vec).allocateNew(rowCount);
                break;
            case "blob":
            case "frozen":
//                Cassandra treats it as a blob, so this should work
//                maybe VarBinaryVector? FixedSizeBinaryVector?
                vec = new LargeVarBinaryVector(colName, allocator);
                ((LargeVarBinaryVector) vec).allocateNew(rowCount);
                break;
            case "boolean":
                vec = new BitVector(colName, allocator);
                ((BitVector) vec).allocateNew(rowCount);
                break;
            case "date":
                vec = new DateDayVector(colName, allocator);
                ((DateDayVector) vec).allocateNew(rowCount);
                break;
            case "decimal":
            case "double":
//                TODO not sure about how to determine precision and scale for decimals yet
//                currently just using floats
                vec = new Float8Vector(colName, allocator);
                ((Float8Vector) vec).allocateNew(rowCount);
                break;
            case "float":
                vec = new Float4Vector(colName, allocator);
                ((Float4Vector) vec).allocateNew(rowCount);
                break;
            case "inet":
                vec = new VarCharVector(colName, allocator);
                ((VarCharVector) vec).allocateNew(rowCount);
                break;
            case "int":
                vec = new IntVector(colName, allocator);
                ((IntVector) vec).allocateNew(rowCount);
                break;
            case "list":
                vec = ListVector.empty(colName, allocator);
//                TODO need to allocate?
                break;
            case "map":
                vec = MapVector.empty(colName, allocator, false);
//                TODO need to allocate?
                break;
            case "set":
//                TODO no such thing as SetVector, custom implementation?
                vec = MapVector.empty(colName, allocator, true);
                break;
            case "smallint":
//                2 bytes
                vec = new SmallIntVector(colName, allocator);
                ((SmallIntVector) vec).allocateNew(rowCount);
                break;
            case "text":
                vec = new LargeVarCharVector(colName, allocator);
                ((LargeVarCharVector) vec).allocateNew(rowCount);
                break;
            case "time":
//                The CQL documentation is a bit ambiguous
//                about whether it's stored as an 8-byte integer or as a string
                vec = new VarCharVector(colName, allocator);
                ((VarCharVector) vec).allocateNew(rowCount);
                break;
            case "timestamp":
//                same ambiguity as above
                vec = new TimeStampMilliVector(colName, allocator);
                ((TimeStampMilliVector) vec).allocateNew(rowCount);
                break;
            case "timeuuid":
            case "uuid":
//                128 bits = 16 bytes, but Arrow doesn't have a data type this big
//                TODO test if can use DecimalVector to implement
                vec = new FixedSizeBinaryVector(colName, allocator, 16);
                ((FixedSizeBinaryVector) vec).allocateNew(rowCount);
                break;
            case "tinyint":
                vec = new TinyIntVector(colName, allocator);
                ((TinyIntVector) vec).allocateNew(rowCount);
                break;
            case "tuple":
//                Maybe use MapVector?
//                TODO need allocate?
                vec = ListVector.empty(colName, allocator);
                break;
            case "varint":
//                TODO Has to be a better implementation than storing in a varchar...
//                maybe varbinary?
                vec = new VarCharVector(colName, allocator);
                ((VarCharVector) vec).allocateNew(rowCount);
                break;
            default:
                System.out.println("ERROR: CQL3 type not implemented: " + cql3Type);
                return null;
        }
//        might need to do stuff with vec?
        return vec;
    }


    /**
     * Assumes that the proper type of value is passed for each type of vector
     *
     * @param col      the column, corresponding to a FieldVector, to insert the value into
     * @param rowIndex the index to insert the value at
     * @param value    the value to insert
     */
    private void appendToVector(ColumnDefinition col, int rowIndex, Object value) {
//        unsure about the difference between this and toSchemaString
//        switching on CQL3/Cassandra/SSTable type and returning an Arrow ValueVector

        FieldVector vec = vectors.get(col.name.toString());
        String cql3Type = col.type.asCQL3Type().toString();
        switch (cql3Type) {
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

    /**
     * Send the filled Arrow FieldVectors as a RecordBatch across a network socket specified by `PORT_NUMBER`
     */
    private static void transferArrowIpc(VectorSchemaRoot root) {
        System.out.println("Listening for socket on port " + PORT_NUMBER);

//        Send data directly on connection
        try (
                ServerSocket serverSocket = new ServerSocket(PORT_NUMBER);
                Socket clientSocket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
        ) {
            System.out.println("connected to socket from address " + clientSocket.getInetAddress());

            // wait for signal from client
            String line = in.readLine();
            System.out.println("Received " + line);

            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(clientSocket.getOutputStream()));

            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
