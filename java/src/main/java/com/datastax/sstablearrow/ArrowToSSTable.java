package com.datastax.sstablearrow;

import com.datastax.cndb.metadata.storage.SSTableCreation;
import com.datastax.cndb.metadata.storage.SSTableData;
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
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.NotImplementedException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.InflaterInputStream;

public class ArrowToSSTable {

    public static final String DATA_PREFIX = "__astra_data__";

    static {
        Util.init();
    }

    public static String getInsertStatement(TableMetadata metadata) {
//        TODO handle uppercase and lowercase, write test
        StringBuilder sb = new StringBuilder("INSERT INTO \"" + metadata.keyspace + "\".\"" + metadata.name + "\" (");
        StringBuilder values = new StringBuilder(" VALUES (");
        boolean first = true;
        for (Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder(); it.hasNext(); ) {
            ColumnMetadata col = it.next();
            if (!first) {
                sb.append(", ");
                values.append(", ");
            }
            sb.append(col.name);
            values.append("?");
            first = false;
        }
        sb.append(")");
        sb.append(values);
        sb.append(")");

        return sb.toString();
    }

    public static String getTenantId(String datacenterId) {
        return datacenterId.substring(0, datacenterId.lastIndexOf('-'));
    }

    /**
     * Fetch the most recent schema file.
     *
     * @param s3           the S3 client
     * @param datacenterId the tenant ID and bucket name, a UUID with a suffixed ordinal
     * @return
     */
    public static JSONObject fetchSchemaFile(S3Client s3, String datacenterId) throws IOException, ParseException {
        String prefix = String.format("metadata_backup/%s/schema", getTenantId(datacenterId));
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(datacenterId)
                .prefix(prefix)
                .build();

        ListObjectsResponse res = s3.listObjects(listObjects);
        String schemaKey = "";
        int recentVersion = 0;
        for (S3Object obj : res.contents()) {
            try {
                String[] filenameParts = obj.key()
                        .split("/");
                int version = Integer.parseInt(filenameParts[filenameParts.length - 1].split("_")[1]);
                if (version > recentVersion) {
                    recentVersion = version;
                    schemaKey = obj.key();
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                continue;
            }
        }

        GetObjectRequest getObject = GetObjectRequest.builder()
                .bucket(datacenterId)
                .key(schemaKey)
                .build();
        InputStream schemaFile = s3.getObjectAsBytes(getObject)
                .asInputStream();
        InflaterInputStream stream = new InflaterInputStream(schemaFile);
        StringBuilder builder = new StringBuilder();
        byte[] buffer = new byte[512];
        int nread = -1;
        while ((nread = stream.read(buffer)) != -1) {
            builder.append(new String(Arrays.copyOf(buffer, nread)));
        }

        JSONParser parser = new JSONParser();
        JSONObject allSchemas = (JSONObject) parser.parse(builder.toString());

        return allSchemas;
    }

    /**
     * @param path A path that ends in {keyspace}/{table}/{filename}
     * @return The last two components of the path before the filename.
     */
    public static Pair<String, String> getKeyspaceAndTableName(Path path) {
        String keyspaceName = path.getParent()
                .getParent()
                .getFileName()
                .toString();
        String tableName = path.getParent()
                .getFileName()
                .toString();
        return Pair.create(keyspaceName, tableName);
    }

    public static TableMetadata getSchema(JSONObject schemaFile, String keyspaceName, String tableName) {
        for (Object obj : ((JSONArray) schemaFile.get("keyspaces"))) {
            JSONObject keyspace = (JSONObject) obj;
            String prefixedKeyspace = (String) keyspace.get("name");
            if (!prefixedKeyspace.endsWith(keyspaceName)) continue;
            JSONArray tables = (JSONArray) keyspace.get("tables");
            for (Object tableObj : tables) {
                JSONObject table = (JSONObject) tableObj;
                if (((String) table.get("name")).endsWith(tableName)) {
                    return CreateTableStatement.parse((String) table.get("create_statement"), prefixedKeyspace)
                            .build();
                }
            }
        }
        return null;
    }

    /**
     * Download all Parquet files from an S3 bucket under the given DATA_PREFIX.
     *
     * @param s3         the initialized S3 client
     * @param bucketName the name of the bucket to download from
     * @return a list of Paths to the Parquet files on the local filesystem
     */
    public static List<Path> downloadAllParquetFiles(S3Client s3, String bucketName) throws S3Exception, IOException {
        List<Path> outputs = new ArrayList<>();

        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(DATA_PREFIX)
                .build();

        ListObjectsResponse res = s3.listObjects(listObjects);
        List<S3Object> objects = res.contents();

        Path dir = Files.createTempDirectory("download-s3-");

        for (S3Object object : objects) {
            String[] parts = object.key()
                    .split("/");
            if (parts.length != 4 || !object.key()
                    .endsWith("parquet")) continue;
            File file = new File(dir + File.separator + object.key());
            file.getParentFile()
                    .mkdirs();
            assert file.getParentFile()
                    .exists();

            GetObjectRequest getObject = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(object.key())
                    .build();

            s3.getObject(getObject, file.toPath());

            outputs.add(file.toPath());
        }

        return outputs;
    }

    public static VectorSchemaRoot readParquetFile(String path) throws Exception {
        BufferAllocator allocator = new RootAllocator();

        DatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, path);
        Dataset dataset = factory.finish();
        Scanner scanner = dataset.newScan(new ScanOptions(100));
        List<ArrowRecordBatch> batches = new ArrayList<>();
        for (ScanTask task : scanner.scan()) {
            for (ScanTask.BatchIterator it = task.execute(); it.hasNext(); ) {
                ArrowRecordBatch batch = it.next();
                batches.add(batch);
            }
        }

        List<Field> fields = factory.inspect()
                .getFields();
        List<FieldVector> vectors = fields.stream()
                .map(field -> field.createVector(allocator))
                .collect(Collectors.toList());
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        VectorLoader loader = new VectorLoader(root);
        batches.forEach(batch -> loader.load(batch));

        AutoCloseables.close(batches);
        AutoCloseables.close(factory, dataset, scanner);

        return root;
    }

    public static Map<ColumnIdentifier, FieldVector> alignSchemas(VectorSchemaRoot root, TableMetadata cassandraMetadata) throws RuntimeException {
        Map<ColumnIdentifier, FieldVector> positionMapping = new HashMap<>();
        List<FieldVector> vectors = root.getFieldVectors();
        Map<ColumnIdentifier, String> errors = new HashMap<>();

        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            ColumnMetadata col = cassandraMetadata.getColumn(ByteBuffer.wrap(vector.getName()
                    .toLowerCase()
                    .getBytes()));
            if (col == null) {
                System.err.println("column " + vector.getName() + " not found in Cassandra table, skipping");
            } else if (!col.type.isValueCompatibleWith(getMatchingCassandraType(vector.getMinorType()))) {
                errors.put(col.name, String.format("types for column %s don't match (MinorType %s (matches %s) is incompatible with CQL3Type %s)\n",
                        col.name,
                        vector.getMinorType(),
                        getMatchingCassandraType(vector.getMinorType()).asCQL3Type()
                                .toString(),
                        col.type.asCQL3Type()
                                .toString()));
            } else {
                positionMapping.put(col.name, vector);
            }
        }

//        check that data is provided for all of the cassandra columns
        List<String> mismatch = cassandraMetadata.columns()
                .stream()
                .map(columnMetadata -> {
                    if (!errors.containsKey(columnMetadata.name) && !positionMapping.containsKey(columnMetadata.name)) {
                        return "no matching column found for " + columnMetadata.name.toString();
                    }
                    return null;
                })
                .filter(col -> col != null)
                .collect(Collectors.toList());

        mismatch.addAll(errors.values());

        if (mismatch.size() > 0) {
            throw new RuntimeException(mismatch.toString());
        }

        return positionMapping;
    }

    /**
     * Writes the given data to an SSTable on disk. Note that the generation will be whatever CQLSSTableWriter generates
     * (1, 2, etc) instead of the ULID expected by CNDB.
     *
     * @param metadata the schema of the Cassandra table
     * @param data     a mapping from Cassandra column identifiers to Arrow field vectors
     * @param dataDir  the directory for the SSTable files to be written to
     * @return a list of SSTableData objects for passing to etcd
     * @throws IOException
     */
    public static List<SSTableData> writeCqlSSTable(TableMetadata metadata, Map<ColumnIdentifier, FieldVector> data, Path dataDir, boolean useUlid) throws IOException, ExecutionException, InterruptedException {
        dataDir = dataDir.resolve(metadata.keyspace + File.separator + metadata.name);
        Files.createDirectories(dataDir);
        assert dataDir.toFile()
                .exists();
        System.out.println("writing SSTables to " + dataDir.toAbsolutePath());

        Descriptor desc = new Descriptor(dataDir.toFile(), metadata.keyspace, metadata.name, Util.ULIDBasedSSTableUniqueIdentifier.fromNextValue());

//        BigTableWriter.create(desc, )



        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(desc.getDirectory()
                        .toFile())
                .forTable(metadata.toCQLCreateStatement())
                .using(getInsertStatement(metadata))
                .build();

        int size = data.values()
                .stream()
                .findFirst()
                .get()
                .getValueCount();

        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < size; i++) {
            row.clear();
            for (ColumnMetadata columnMetadata : metadata.columns()) {
                FieldVector vector = data.get(columnMetadata.name);
//                TODO handle value conversions
                Object obj = toJavaClass(vector, i);

//                https://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/DataType.Name.html#asJavaClass--

//                ByteBufferUtil.UNSET_BYTE_BUFFER
                if (obj == null) {

                }

                row.put(columnMetadata.name.toString(), obj);
            }
            writer.addRow(row);
        }

        writer.close();

//        if (useUlid) {
//            for (File file : dataDir.toFile()
//                    .listFiles()) {
//                Files.move(file.toPath(), desc.pathFor(Descriptor.componentFromFilename(file)));
//            }
//        }

        SSTableLoader loader = new SSTableLoader(dataDir.toFile(), new SSTableToArrow.TestClient(), new OutputHandler.SystemOutput(true, true));
        loader.stream()
                .get();
        List<SSTableReader> readers = loader.streamedSSTables();

        return readers.stream()
                .map(ssTableReader -> SSTableData.create(ssTableReader, SSTableCreation.withRandomId(OperationType.UNKNOWN)))
                .collect(Collectors.toList());
    }

    public static Object toJavaClass(FieldVector vector, int index) {
        switch (vector.getMinorType()) {
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

    public static AbstractType getMatchingCassandraType(Types.MinorType minorType) {
        switch (minorType) {
            case NULL:
                throw new NotImplementedException("Not implemented");
            case STRUCT:
                throw new NotImplementedException("Not implemented");
            case TINYINT:
                return ByteType.instance;
            case SMALLINT:
                return ShortType.instance;
            case INT:
                return Int32Type.instance;
            case BIGINT:
                return LongType.instance;
            case DATEDAY:
                return TimestampType.instance;
            case DATEMILLI:
                return TimestampType.instance;
            case TIMESEC:
                return TimeType.instance;
            case TIMEMILLI:
                return TimeType.instance;
            case TIMEMICRO:
                return TimeType.instance;
            case TIMENANO:
                return TimeType.instance;
            case TIMESTAMPSEC:
                return TimestampType.instance;
            case TIMESTAMPMILLI:
                return TimestampType.instance;
            case TIMESTAMPMICRO:
                return TimestampType.instance;
            case TIMESTAMPNANO:
                return TimestampType.instance;
            case INTERVALDAY:
                return DurationType.instance;
            case DURATION:
                return DurationType.instance;
            case INTERVALYEAR:
                return DurationType.instance;
            case FLOAT4:
                return FloatType.instance;
            case FLOAT8:
                return DoubleType.instance;
            case BIT:
                return BooleanType.instance;
            case VARCHAR:
                return UTF8Type.instance;
            case LARGEVARCHAR:
                return UTF8Type.instance;
            case LARGEVARBINARY:
                return BytesType.instance;
            case VARBINARY:
                return BytesType.instance;
            case DECIMAL:
                return DecimalType.instance;
            case DECIMAL256:
                return DecimalType.instance;
            case FIXEDSIZEBINARY:
                return BytesType.instance;
            case UINT1:
                return ShortType.instance;
            case UINT2:
                return Int32Type.instance;
            case UINT4:
                return LongType.instance;
            case UINT8:
                return IntegerType.instance;
            case LIST:
                throw new NotImplementedException("not implemented");
            case LARGELIST:
                throw new NotImplementedException("not implemented");
            case FIXED_SIZE_LIST:
                throw new NotImplementedException("not implemented");
            case UNION:
                throw new NotImplementedException("not implemented");
            case DENSEUNION:
                throw new NotImplementedException("not implemented");
            case MAP:
                throw new NotImplementedException("not implemented");
            case TIMESTAMPSECTZ:
                return TimestampType.instance;
            case TIMESTAMPMILLITZ:
                return TimestampType.instance;
            case TIMESTAMPMICROTZ:
                return TimestampType.instance;
            case TIMESTAMPNANOTZ:
                return TimestampType.instance;
            case EXTENSIONTYPE:
                throw new NotImplementedException("not implemented");
            default:
                throw new RuntimeException("Unrecognized type");
        }
    }
}
