package com.datastax.sstablearrow;

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
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
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
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class ArrowToSSTable {

    public static final String DATA_PREFIX = "__astra_data__";

    public static String getInsertStatement(TableMetadata metadata) {
        StringBuilder sb = new StringBuilder("INSERT INTO " + metadata.keyspace + "." + metadata.name + " (");
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

    /**
     * Fetch the most recent schema file.
     *
     * @param s3
     * @param tenantId the tenant ID and bucket name, a UUID with a suffixed ordinal
     * @return
     */
    public static Object fetchSchema(S3Client s3, String tenantId, String keyspaceName, String tableName) throws IOException, ParseException {
        String prefix = String.format("metadata_backup/%s/schema", tenantId.substring(0, tenantId.lastIndexOf('-')));
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(tenantId)
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
                .bucket(tenantId)
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
        for (Object obj : ((JSONArray) allSchemas.get("keyspaces"))) {
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
    public static List<Path> downloadAllParquetFiles(S3Client s3, String bucketName) {
        List<Path> outputs = new ArrayList<>();

        try {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    public static Map<ColumnIdentifier, FieldVector> alignSchemas(VectorSchemaRoot root, TableMetadata cassandraMetadata) {
        Map<ColumnIdentifier, FieldVector> positionMapping = new HashMap<>();
        List<FieldVector> vectors = root.getFieldVectors();

        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            ColumnMetadata col = cassandraMetadata.getColumn(ByteBuffer.wrap(vector.getName()
                    .getBytes()));
            if (col == null) {
                System.err.println("column " + vector.getName() + " not found in Cassandra table, skipping");
            } else if (!ArrowTransferUtil.validType(col.type, vector.getMinorType())) {
                System.err.printf("types for column %s don't match (CQL3Type %s (matches %s) != MinorType %s)\n", col.name, col.type.asCQL3Type()
                        .toString(), ArrowTransferUtil.matchingType(col.type), vector.getMinorType());
            } else {
                positionMapping.put(col.name, vector);
            }
        }

        String[] mismatch = cassandraMetadata.columns()
                .stream()
                .map(columnMetadata -> {
                    if (!positionMapping.containsKey(columnMetadata.name)) {
                        return columnMetadata.name.toString();
                    }
                    return null;
                })
                .filter(col -> col != null)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        if (mismatch.length > 0) {
            throw new RuntimeException("no matching columns found in the Parquet file for Cassandra columns " + Arrays.toString(mismatch));
        }

        return positionMapping;
    }

    /**
     * Writes the given data to an SSTable on disk.
     *
     * @param metadata the schema of the Cassandra table
     * @param data     a mapping from Cassandra column identifiers to Arrow field vectors
     * @param dataDir  the directory for the SSTable files to be written to
     * @throws IOException
     */
    public static void writeCqlSSTable(TableMetadata metadata, Map<ColumnIdentifier, FieldVector> data, Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        assert dataDir.toFile()
                .exists();
        System.out.println("writing SSTables to " + dataDir.toAbsolutePath());

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(dataDir.toFile())
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
                row.put(columnMetadata.name.toString(), data.get(columnMetadata.name)
                        .getObject(i));
            }
            writer.addRow(row);
        }

        writer.close();
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
