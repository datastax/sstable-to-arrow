package com.datastax.sstablearrow;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Class for converting Arrow Tables to SSTable files on disk.
 */
public class ArrowToSSTable
{

    public static final String DATA_PREFIX = "__astra_data__";
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowToSSTable.class);

    /**
     * Get a keyspace and table name from the path to the SSTable files within the bucket.
     *
     * @param path A path that ends in {keyspace}/{table}/{filename}
     *
     * @return The last two components of the path before the filename.
     */
    public static Pair<String, String> getKeyspaceAndTableName(Path path)
    {
        String keyspaceName = path.getParent()
                .getParent()
                .getFileName()
                .toString();
        String tableName = path.getParent()
                .getFileName()
                .toString();
        return Pair.create(keyspaceName, tableName);
    }

    public static TableMetadata getSchema(JSONObject schemaFile, String keyspaceName, String tableName)
    {
        for (Object obj : ((JSONArray) schemaFile.get("keyspaces")))
        {
            JSONObject keyspace = (JSONObject) obj;
            String prefixedKeyspace = (String) keyspace.get("name");
            if (!prefixedKeyspace.endsWith(keyspaceName)) continue;
            JSONArray tables = (JSONArray) keyspace.get("tables");
            for (Object tableObj : tables)
            {
                JSONObject table = (JSONObject) tableObj;
                if (((String) table.get("name")).endsWith(tableName))
                {
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
     * @param s3 the initialized S3 client
     * @param bucketName the name of the bucket to download from
     *
     * @return a list of Paths to the Parquet files on the local filesystem
     */
    public static List<S3Object> listParquetFiles(S3Client s3, String bucketName, String prefix) throws S3Exception
    {
        LOGGER.debug("Listing Parquet files in bucket {} under {}", bucketName, prefix);
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsResponse res = s3.listObjects(listObjects);

        LOGGER.debug("Found {} files in bucket {} under {}", res.contents().size(), bucketName, prefix);

        List<S3Object> parquetObjects = res.contents().stream().filter(object -> {
            String[] parts = object.key().split("/");
            return parts.length >= 4 && object.key().endsWith("parquet");
        }).collect(Collectors.toList());

        LOGGER.debug("Found {} Parquet files in bucket {} under {}: {}",
                parquetObjects.size(), bucketName, prefix,
                parquetObjects.stream().map(S3Object::key).collect(Collectors.toList()));

        return parquetObjects;
    }

    public static List<S3Object> listParquetFiles(S3Client s3, String bucketName) throws S3Exception
    {
        return listParquetFiles(s3, bucketName, DATA_PREFIX);
    }

    /**
     * Downloads the given object from S3 into the given directory, preserving the relative path.
     *
     * @param s3 the S3 client to query with
     * @param bucket the name of the bucket that the object resides in
     * @param object the name of the object to download
     * @param outdir the directory to download the object to
     *
     * @return the path to the downloaded object
     */
    public static Path downloadFile(S3Client s3, String bucket, S3Object object, Path outdir)
    {
        Path output = outdir.resolve(object.key());

        LOGGER.debug("Downloading {} from bucket {} to {}", object.key(), bucket, output);

        GetObjectRequest getObject = GetObjectRequest.builder()
                .bucket(bucket)
                .key(object.key())
                .build();

        s3.getObject(getObject, output);

        LOGGER.debug("Downloaded {} from bucket {} to {}", object.key(), bucket, output);

        return output;
    }

    public static Map<ColumnIdentifier, FieldVector> alignSchemas(VectorSchemaRoot root, TableMetadata cassandraMetadata) throws RuntimeException
    {
        LOGGER.debug("Aligning root with columns {} to Cassandra table {}.{} with columns {}",
                root.getFieldVectors().stream().map(ValueVector::getName).collect(Collectors.toList()),
                cassandraMetadata.keyspace, cassandraMetadata.name,
                cassandraMetadata.columns().stream().map(column -> column.name).collect(Collectors.toList()));

        Map<ColumnIdentifier, FieldVector> positionMapping = new HashMap<>();
        List<FieldVector> vectors = root.getFieldVectors();
        Map<ColumnIdentifier, String> errors = new HashMap<>();

        for (int i = 0; i < vectors.size(); i++)
        {
            FieldVector vector = vectors.get(i);
            ColumnMetadata col = cassandraMetadata.getColumn(ByteBuffer.wrap(vector.getName()
                    .toLowerCase()
                    .getBytes()));
            if (col == null)
            {
                LOGGER.error("column " + vector.getName() + " not found in Cassandra table, skipping");
            }
            else if (!col.type.isValueCompatibleWith(getMatchingCassandraType(vector.getMinorType())))
            {
                errors.put(col.name, String.format("types for column %s don't match (MinorType %s (matches %s) is incompatible with CQL3Type %s)\n",
                        col.name,
                        vector.getMinorType(),
                        getMatchingCassandraType(vector.getMinorType()).asCQL3Type()
                                .toString(),
                        col.type.asCQL3Type()
                                .toString()));
            }
            else
            {
                positionMapping.put(col.name, vector);
            }
        }

        LOGGER.debug("Done mapping columns for Cassandra table {}.{}", cassandraMetadata.keyspace, cassandraMetadata.name);

        //        check that data is provided for all of the cassandra columns
        List<String> mismatch = cassandraMetadata.columns()
                .stream()
                .map(columnMetadata -> {
                    if (!errors.containsKey(columnMetadata.name) && !positionMapping.containsKey(columnMetadata.name))
                    {
                        return "no matching column found for " + columnMetadata.name.toString();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        mismatch.addAll(errors.values());

        LOGGER.debug("Errors aligning schemas: {}", mismatch);

        if (mismatch.size() > 0)
        {
            throw new RuntimeException(mismatch.toString());
        }

        return positionMapping;
    }

    //    public static void writeSSTable() {
    //        BigTableWriter writer = BigTableWriter.create(new Descriptor())
    //    }

    /**
     * Creates a Cassandra table with the same schema as the given Arrow table.
     *
     * @param root the Arrow table whose schema will be used to create the Cassandra table.
     *
     * @return a schema for the Cassandra table whose columns correspond to the Arrow table columns.
     */
    public static TableMetadata createMatchingTable(VectorSchemaRoot root, String keyspace, String table, boolean excludeKey)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table);
        builder.addPartitionKeyColumn("key", UTF8Type.instance);
        root.getFieldVectors()
                .forEach(vector -> {
                    // ignore the key column
                    if (excludeKey && vector.getName().equals("key")) return;
                    builder.addRegularColumn(
                            vector.getName(),
                            ArrowToSSTable.getMatchingCassandraType(vector.getMinorType()));
                });
        return builder.build();
    }

    public static AbstractType getMatchingCassandraType(Types.MinorType minorType)
    {
        switch (minorType)
        {
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
