/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.utils.Throwables;
import org.junit.After;
import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.ViewTableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MapsFactory;
import org.apache.cassandra.utils.time.ApolloTime;

/**
 * Legacy test utility: this is still used by a number of tests but is discouraged for new tests as it is focused on
 * creating tables with specific layouts that made sense in the legacy storage engine but are somewhat arbitrary/weird
 * now (also, tests are more readable if the layout of the table they use is defined in the tests themselves). This
 * also have a few methods unrelated to schema that just scream of poor organization.
 *
 * <p>Instead, for simple manipulation of the schema, prefer SchemaTestUtils, or use {@link CQLTester} directly
 * if the test is a more full-featured "full-stack" test.
 *
 * <p>If you work on a test class that uses it, feel free to take some time to migrate it off this class if it's not
 * much work and you feel in a cleaning mood.
 */
public class SchemaLoader
{
    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        prepareServer();

        // Migrations aren't happy if gossiper is not started.  Even if we don't use migrations though,
        // some tests now expect us to start gossip for them.
        startGossiper();
    }

    @After
    public void leakDetect() throws InterruptedException
    {
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(10);
    }

    public static void prepareServer()
    {
        prepareServer(true);
    }

    public static void prepareServer(boolean restartCommitLog)
    {
        CQLTester.prepareServer(restartCommitLog);
    }

    public static void startGossiper()
    {
        // skip shadow round and endpoint collision check in tests
        System.setProperty("cassandra.allow_unsafe_join", "true");
        if (!Gossiper.instance.isEnabled())
            Gossiper.instance.startUnsafe();
    }

    public static void schemaDefinition(String testName) throws ConfigurationException
    {
        List<KeyspaceMetadata> schema = new ArrayList<KeyspaceMetadata>();

        // A whole bucket of shorthand
        String ks1 = testName + "Keyspace1";
        String ks2 = testName + "Keyspace2";
        String ks3 = testName + "Keyspace3";
        String ks4 = testName + "Keyspace4";
        String ks5 = testName + "Keyspace5";
        String ks6 = testName + "Keyspace6";
        String ks7 = testName + "Keyspace7";
        String ks_rcs = testName + "RowCacheSpace";
        String ks_ccs = testName + "CounterCacheSpace";
        String ks_nocommit = testName + "NoCommitlogSpace";
        String ks_prsi = testName + "PerRowSecondaryIndex";
        String ks_cql = testName + "cql_keyspace";

        AbstractType bytes = BytesType.instance;

        AbstractType<?> composite = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, TimeUUIDType.instance, IntegerType.instance}));
        AbstractType<?> compositeMaxMin = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, IntegerType.instance}));
        Map<Byte, AbstractType<?>> aliases = MapsFactory.newMap();
        aliases.put((byte)'b', BytesType.instance);
        aliases.put((byte)'t', TimeUUIDType.instance);
        aliases.put((byte)'B', ReversedType.getInstance(BytesType.instance));
        aliases.put((byte)'T', ReversedType.getInstance(TimeUUIDType.instance));
        AbstractType<?> dynamicComposite = DynamicCompositeType.getInstance(aliases);

        // Make it easy to test compaction
        Map<String, String> compactionOptions = MapsFactory.newMap();
        compactionOptions.put("tombstone_compaction_interval", "1");
        Map<String, String> leveledOptions = MapsFactory.newMap();
        leveledOptions.put("sstable_size_in_mb", "1");
        leveledOptions.put("fanout_size", "5");

        // Keyspace 1
        schema.add(KeyspaceMetadata.create(ks1,
                KeyspaceParams.simple(1),
                Tables.of(
                        // Column Families
                        standardCFMD(ks1, "Standard1").compaction(CompactionParams.scts(compactionOptions)).build(),
                        standardCFMD(ks1, "Standard2").build(),
                        standardCFMD(ks1, "Standard3").build(),
                        standardCFMD(ks1, "Standard4").build(),
                        standardCFMD(ks1, "StandardGCGS0").gcGraceSeconds(0).build(),
                        standardCFMD(ks1, "StandardLong1").build(),
                        standardCFMD(ks1, "StandardLong2").build(),
                        superCFMD(ks1, "Super1", LongType.instance).build(),
                        superCFMD(ks1, "Super2", LongType.instance).build(),
                        superCFMD(ks1, "Super3", LongType.instance).build(),
                        superCFMD(ks1, "Super4", UTF8Type.instance).build(),
                        superCFMD(ks1, "Super5", bytes).build(),
                        superCFMD(ks1, "Super6", LexicalUUIDType.instance, UTF8Type.instance).build(),
                        keysIndexCFMD(ks1, "Indexed1", true).build(),
                        keysIndexCFMD(ks1, "Indexed2", false).build(),
                        superCFMD(ks1, "SuperDirectGC", BytesType.instance).gcGraceSeconds(0).build(),
                        jdbcCFMD(ks1, "JdbcUtf8", UTF8Type.instance).addColumn(utf8Column(ks1, "JdbcUtf8")).build(),
                        jdbcCFMD(ks1, "JdbcLong", LongType.instance).build(),
                        jdbcCFMD(ks1, "JdbcBytes", bytes).build(),
                        jdbcCFMD(ks1, "JdbcAscii", AsciiType.instance).build(),
                        standardCFMD(ks1, "StandardLeveled").compaction(CompactionParams.lcs(leveledOptions)).build(),
                        standardCFMD(ks1, "legacyleveled").compaction(CompactionParams.lcs(leveledOptions)).build(),
                        standardCFMD(ks1, "StandardLowIndexInterval").minIndexInterval(8)
                                .maxIndexInterval(256)
                                .caching(CachingParams.CACHE_NOTHING).build()
                )));

        // Keyspace 2
        schema.add(KeyspaceMetadata.create(ks2,
                KeyspaceParams.simple(1),
                Tables.of(
                        // Column Families
                        standardCFMD(ks2, "Standard1").build(),
                        standardCFMD(ks2, "Standard3").build(),
                        superCFMD(ks2, "Super3", bytes).build(),
                        superCFMD(ks2, "Super4", TimeUUIDType.instance).build(),
                        keysIndexCFMD(ks2, "Indexed1", true).build(),
                        compositeIndexCFMD(ks2, "Indexed2", true).build(),
                        compositeIndexCFMD(ks2, "Indexed3", true).gcGraceSeconds(0).build())));

        // Keyspace 3
        schema.add(KeyspaceMetadata.create(ks3,
                KeyspaceParams.simple(5),
                Tables.of(
                        standardCFMD(ks3, "Standard1").build(),
                        keysIndexCFMD(ks3, "Indexed1", true).build())));

        // Keyspace 4
        schema.add(KeyspaceMetadata.create(ks4,
                KeyspaceParams.simple(3),
                Tables.of(
                        standardCFMD(ks4, "Standard1").build(),
                        standardCFMD(ks4, "Standard3").build(),
                        superCFMD(ks4, "Super3", bytes).build(),
                        superCFMD(ks4, "Super4", TimeUUIDType.instance).build(),
                        superCFMD(ks4, "Super5", TimeUUIDType.instance, BytesType.instance).build())));

        // Keyspace 5
        schema.add(KeyspaceMetadata.create(ks5,
                KeyspaceParams.simple(2),
                Tables.of(standardCFMD(ks5, "Standard1").build())));

        // Keyspace 6
        schema.add(KeyspaceMetadata.create(ks6,
                KeyspaceParams.simple(1),
                Tables.of(keysIndexCFMD(ks6, "Indexed1", true).build())));

        // Keyspace 7
        schema.add(KeyspaceMetadata.create(ks7,
                KeyspaceParams.simple(1),
                Tables.of(customIndexCFMD(ks7, "Indexed1").build())));

        // RowCacheSpace
        schema.add(KeyspaceMetadata.create(ks_rcs,
                KeyspaceParams.simple(1),
                Tables.of(
                        standardCFMD(ks_rcs, "CFWithoutCache").caching(CachingParams.CACHE_NOTHING).build(),
                        standardCFMD(ks_rcs, "CachedCF").caching(CachingParams.CACHE_EVERYTHING).build(),
                        standardCFMD(ks_rcs, "CachedNoClustering", 1, IntegerType.instance, IntegerType.instance, null).caching(CachingParams.CACHE_EVERYTHING).build(),
                        standardCFMD(ks_rcs, "CachedIntCF").caching(new CachingParams(true, 100)).build())));

        schema.add(KeyspaceMetadata.create(ks_nocommit, KeyspaceParams.simpleTransient(1), Tables.of(
                standardCFMD(ks_nocommit, "Standard1").build())));

        // CQLKeyspace
        schema.add(KeyspaceMetadata.create(ks_cql, KeyspaceParams.simple(1), Tables.of(

                // Column Families
                CreateTableStatement.parse("CREATE TABLE table1 ("
                                + "k int PRIMARY KEY,"
                                + "v1 text,"
                                + "v2 int"
                                + ")", ks_cql)
                        .build(),

                CreateTableStatement.parse("CREATE TABLE table2 ("
                                + "k text,"
                                + "c text,"
                                + "v text,"
                                + "PRIMARY KEY (k, c))", ks_cql)
                        .build()
        )));

        for (KeyspaceMetadata ksm : schema)
            doSchemaChanges(SchemaTransformations.createKeyspaceIfNotExists(ksm));

        if (Boolean.parseBoolean(System.getProperty("cassandra.test.encryption", "false")))
            useEncryption(schema);
        else if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            useCompression(schema);
    }

    public static SchemaTransformation createView(String ks, String baseTableName, String mvName)
    {
        TableMetadata baseCfm = SchemaManager.instance.getTableMetadata(ks, baseTableName);
        return createView(baseCfm, mvName);
    }

    public static SchemaTransformation createView(TableMetadata baseCfm, String mvName)
    {
        return createView(mvName, baseCfm, AsciiType.instance, AsciiType.instance, AsciiType.instance);
    }

    public static SchemaTransformation createView(String viewName, TableMetadata baseCfm, AbstractType<?> keyType,
                                                  AbstractType<?> valType, AbstractType<?> clusteringType)
    {
        StringBuilder whereClauseBuilder = new StringBuilder();
        Iterator<ColumnMetadata> columns = baseCfm.columns().iterator();
        whereClauseBuilder.append(String.format("%s IS NOT NULL", columns.next().name.toCQLString()));
        while (columns.hasNext())
        {
            whereClauseBuilder.append(String.format(" AND %S IS NOT NULL", columns.next().name.toCQLString()));
        }

        ViewTableMetadata.ViewBuilder builder = ViewTableMetadata.builder(baseCfm, viewName)
                .addPartitionKeyColumn("val", keyType)
                .addPartitionKeyColumn("key", valType)
                .includeAllColumns(true)
                .viewVersion(ViewTableMetadata.CURRENT_VERSION);

        if(clusteringType != null)
            builder.addClusteringColumn("name", clusteringType);

        builder.addRegularColumn(ColumnIdentifier.getInterned(".val", false), AsciiType.instance, true, true);

        try
        {
            builder.whereClause(WhereClause.parse(whereClauseBuilder.toString()));
        }
        catch (RecognitionException e)
        {
            throw new RuntimeException("Problem parsing the where clause " + whereClauseBuilder.toString());
        }

        ViewTableMetadata view = builder.build();
        return SchemaTransformations.createView(view);
    }

    public static ColumnMetadata integerColumn(String ksName, String cfName)
    {
        return new ColumnMetadata(ksName,
                cfName,
                ColumnIdentifier.getInterned(IntegerType.instance.fromString("42"), IntegerType.instance),
                UTF8Type.instance,
                ColumnMetadata.NO_POSITION,
                ColumnMetadata.Kind.REGULAR);
    }

    public static ColumnMetadata utf8Column(String ksName, String cfName)
    {
        return new ColumnMetadata(ksName,
                cfName,
                ColumnIdentifier.getInterned("fortytwo", true),
                UTF8Type.instance,
                ColumnMetadata.NO_POSITION,
                ColumnMetadata.Kind.REGULAR);
    }

    public static TableMetadata perRowIndexedCFMD(String ksName, String cfName)
    {
        ColumnMetadata indexedColumn = ColumnMetadata.regularColumn(ksName, cfName, "indexed", AsciiType.instance);

        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .addPartitionKeyColumn("key", AsciiType.instance)
                        .addColumn(indexedColumn);

        final Map<String, String> indexOptions = Collections.singletonMap(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName());
        builder.indexes(Indexes.of(IndexMetadata.fromIndexTargets(
                Collections.singletonList(new IndexTarget(indexedColumn.name,
                        IndexTarget.Type.VALUES)),
                "indexe1",
                IndexMetadata.Kind.CUSTOM,
                indexOptions)));

        return builder.build();
    }

    private static void useCompression(List<KeyspaceMetadata> schema)
    {
        List<SchemaTransformation> changes = new ArrayList<>();
        for (KeyspaceMetadata ksm : schema)
            for (TableMetadata cfm : ksm.tablesAndViews())
                changes.add(SchemaTransformations.alterTable(cfm.keyspace,
                        cfm.name,
                        b -> b.compression(CompressionParams.snappy())));

        doSchemaChanges(changes);


    }

    private static void useEncryption(List<KeyspaceMetadata> schema)
    {
        final CompressionParams encryption = CompressionParams.forSystemTables();
        assert encryption.isEnabled() && encryption.getOtherOptions().containsKey(CompressionParams.CIPHER_ALGORITHM)
                : "Please set system_info_encryption in the yaml to use encrypted tests.";
        List<SchemaTransformation> changes = new ArrayList<>();
        for (KeyspaceMetadata ksm : schema)
            for (TableMetadata cfm : ksm.tablesAndViews())
                changes.add(SchemaTransformations.alterTable(cfm.keyspace,
                        cfm.name,
                        b -> b.compression(encryption)));
        doSchemaChanges(changes);
    }

    public static TableMetadata.Builder counterCFMD(String ksName, String cfName)
    {
        return TableMetadata.builder(ksName, cfName)
                .isCounter(true)
                .addPartitionKeyColumn("key", AsciiType.instance)
                .addClusteringColumn("name", AsciiType.instance)
                .addRegularColumn("val", CounterColumnType.instance)
                .addRegularColumn("val2", CounterColumnType.instance)
                .compression(getCompressionParameters());
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName)
    {
        return standardCFMD(ksName, cfName, 1, AsciiType.instance);
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType)
    {
        return standardCFMD(ksName, cfName, columnCount, keyType, AsciiType.instance);
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType, AbstractType<?> valType)
    {
        return standardCFMD(ksName, cfName, columnCount, keyType, valType, AsciiType.instance);
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType, AbstractType<?> valType, AbstractType<?> clusteringType)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .addPartitionKeyColumn("key", keyType)
                        .addRegularColumn("val", valType)
                        .compression(getCompressionParameters());

        if (clusteringType != null)
            builder.addClusteringColumn("name", clusteringType);

        for (int i = 0; i < columnCount; i++)
            builder.addRegularColumn("val" + i, AsciiType.instance);

        return builder;
    }


    public static TableMetadata.Builder denseCFMD(String ksName, String cfName)
    {
        return denseCFMD(ksName, cfName, AsciiType.instance);
    }
    public static TableMetadata.Builder denseCFMD(String ksName, String cfName, AbstractType cc)
    {
        return denseCFMD(ksName, cfName, cc, null);
    }
    public static TableMetadata.Builder denseCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        AbstractType comp = cc;
        if (subcc != null)
            comp = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{cc, subcc}));

        return TableMetadata.builder(ksName, cfName)
                .isDense(true)
                .isCompound(subcc != null)
                .addPartitionKeyColumn("key", AsciiType.instance)
                .addClusteringColumn("cols", comp)
                .addRegularColumn("val", AsciiType.instance)
                .compression(getCompressionParameters());
    }

    // TODO: Fix superCFMD failing on legacy table creation. Seems to be applying composite comparator to partition key
    public static TableMetadata.Builder superCFMD(String ksName, String cfName, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, BytesType.instance, subcc);
    }
    public static TableMetadata.Builder superCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, "cols", cc, subcc);
    }
    public static TableMetadata.Builder superCFMD(String ksName, String cfName, String ccName, AbstractType cc, AbstractType subcc)
    {
        return standardCFMD(ksName, cfName);

    }
    public static TableMetadata.Builder compositeIndexCFMD(String ksName, String cfName, boolean withRegularIndex) throws ConfigurationException
    {
        return compositeIndexCFMD(ksName, cfName, withRegularIndex, false);
    }

    public static TableMetadata.Builder compositeIndexCFMD(String ksName, String cfName, boolean withRegularIndex, boolean withStaticIndex) throws ConfigurationException
    {
        // the withIndex flag exists to allow tests index creation
        // on existing columns
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .addPartitionKeyColumn("key", AsciiType.instance)
                        .addClusteringColumn("c1", AsciiType.instance)
                        .addRegularColumn("birthdate", LongType.instance)
                        .addRegularColumn("notbirthdate", LongType.instance)
                        .addStaticColumn("static", LongType.instance)
                        .compression(getCompressionParameters());

        Indexes.Builder indexes = Indexes.builder();

        if (withRegularIndex)
        {
            indexes.add(IndexMetadata.fromIndexTargets(
                    Collections.singletonList(
                            new IndexTarget(new ColumnIdentifier("birthdate", true),
                                    IndexTarget.Type.VALUES)),
                    cfName + "_birthdate_key_index",
                    IndexMetadata.Kind.COMPOSITES,
                    Collections.EMPTY_MAP));
        }

        if (withStaticIndex)
        {
            indexes.add(IndexMetadata.fromIndexTargets(
                    Collections.singletonList(
                            new IndexTarget(new ColumnIdentifier("static", true),
                                    IndexTarget.Type.VALUES)),
                    cfName + "_static_index",
                    IndexMetadata.Kind.COMPOSITES,
                    Collections.EMPTY_MAP));
        }

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder keysIndexCFMD(String ksName, String cfName, boolean withIndex)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .isCompound(false)
                        .isDense(true)
                        .addPartitionKeyColumn("key", AsciiType.instance)
                        .addClusteringColumn("c1", AsciiType.instance)
                        .addStaticColumn("birthdate", LongType.instance)
                        .addStaticColumn("notbirthdate", LongType.instance)
                        .addRegularColumn("value", LongType.instance)
                        .compression(getCompressionParameters());

        if (withIndex)
        {
            IndexMetadata index =
                    IndexMetadata.fromIndexTargets(
                            Collections.singletonList(new IndexTarget(new ColumnIdentifier("birthdate", true),
                                    IndexTarget.Type.VALUES)),
                            cfName + "_birthdate_composite_index",
                            IndexMetadata.Kind.KEYS,
                            Collections.EMPTY_MAP);
            builder.indexes(Indexes.builder().add(index).build());
        }

        return builder;
    }

    public static TableMetadata.Builder customIndexCFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder  =
                TableMetadata.builder(ksName, cfName)
                        .isCompound(false)
                        .isDense(true)
                        .addPartitionKeyColumn("key", AsciiType.instance)
                        .addClusteringColumn("c1", AsciiType.instance)
                        .addRegularColumn("value", LongType.instance)
                        .compression(getCompressionParameters());

        IndexMetadata index =
                IndexMetadata.fromIndexTargets(
                        Collections.singletonList(new IndexTarget(new ColumnIdentifier("value", true), IndexTarget.Type.VALUES)),
                        cfName + "_value_index",
                        IndexMetadata.Kind.CUSTOM,
                        Collections.singletonMap(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName()));

        builder.indexes(Indexes.of(index));

        return builder;
    }

    public static TableMetadata.Builder jdbcCFMD(String ksName, String cfName, AbstractType comp)
    {
        return TableMetadata.builder(ksName, cfName)
                .addPartitionKeyColumn("key", BytesType.instance)
                .compression(getCompressionParameters());
    }

    public static TableMetadata.Builder sasiCFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .addPartitionKeyColumn("id", UTF8Type.instance)
                        .addRegularColumn("first_name", UTF8Type.instance)
                        .addRegularColumn("last_name", UTF8Type.instance)
                        .addRegularColumn("age", Int32Type.instance)
                        .addRegularColumn("height", Int32Type.instance)
                        .addRegularColumn("timestamp", LongType.instance)
                        .addRegularColumn("address", UTF8Type.instance)
                        .addRegularColumn("score", DoubleType.instance)
                        .addRegularColumn("comment", UTF8Type.instance)
                        .addRegularColumn("comment_suffix_split", UTF8Type.instance)
                        .addRegularColumn("/output/full-name/", UTF8Type.instance)
                        .addRegularColumn("/data/output/id", UTF8Type.instance)
                        .addRegularColumn("first_name_prefix", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_first_name", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "first_name",
                        "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString())))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_last_name", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "last_name",
                        "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString())))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_age", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "age")))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_timestamp", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "timestamp",
                        "mode", OnDiskIndexBuilder.Mode.SPARSE.toString())))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_address", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        "analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer",
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "address",
                        "mode", OnDiskIndexBuilder.Mode.PREFIX.toString(),
                        "case_sensitive", "false")))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_score", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "score")))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_comment", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "comment",
                        "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString(),
                        "analyzed", "true")))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_comment_suffix_split", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "comment_suffix_split",
                        "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString(),
                        "analyzed", "false")))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_output_full_name", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "/output/full-name/",
                        "analyzed", "true",
                        "analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer",
                        "case_sensitive", "false")))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_data_output_id", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "/data/output/id",
                        "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString())))
                .add(IndexMetadata.fromSchemaMetadata(cfName + "_first_name_prefix", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                        IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                        IndexTarget.TARGET_OPTION_NAME, "first_name_prefix",
                        "analyzed", "true",
                        "tokenization_normalize_lowercase", "true")));

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder clusteringSASICFMD(String ksName, String cfName)
    {
        return clusteringSASICFMD(ksName, cfName, "location", "age", "height", "score");
    }

    public static TableMetadata.Builder clusteringSASICFMD(String ksName, String cfName, String...indexedColumns)
    {
        Indexes.Builder indexes = Indexes.builder();
        for (String indexedColumn : indexedColumns)
        {
            indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_" + indexedColumn, IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                    IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                    IndexTarget.TARGET_OPTION_NAME, indexedColumn,
                    "mode", OnDiskIndexBuilder.Mode.PREFIX.toString())));
        }

        return TableMetadata.builder(ksName, cfName)
                .addPartitionKeyColumn("name", UTF8Type.instance)
                .addClusteringColumn("location", UTF8Type.instance)
                .addClusteringColumn("age", Int32Type.instance)
                .addRegularColumn("height", Int32Type.instance)
                .addRegularColumn("score", DoubleType.instance)
                .addStaticColumn("nickname", UTF8Type.instance)
                .indexes(indexes.build());
    }

    public static TableMetadata.Builder staticSASICFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .addPartitionKeyColumn("sensor_id", Int32Type.instance)
                        .addStaticColumn("sensor_type", UTF8Type.instance)
                        .addClusteringColumn("date", LongType.instance)
                        .addRegularColumn("value", DoubleType.instance)
                        .addRegularColumn("variance", Int32Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_sensor_type", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                IndexTarget.TARGET_OPTION_NAME, "sensor_type",
                "mode", OnDiskIndexBuilder.Mode.PREFIX.toString(),
                "analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer",
                "case_sensitive", "false")));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_value", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                IndexTarget.TARGET_OPTION_NAME, "value",
                "mode", OnDiskIndexBuilder.Mode.PREFIX.toString())));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_variance", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                IndexTarget.TARGET_OPTION_NAME, "variance",
                "mode", OnDiskIndexBuilder.Mode.PREFIX.toString())));

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder fullTextSearchSASICFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
                TableMetadata.builder(ksName, cfName)
                        .addPartitionKeyColumn("song_id", UUIDType.instance)
                        .addRegularColumn("title", UTF8Type.instance)
                        .addRegularColumn("artist", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_title", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                IndexTarget.TARGET_OPTION_NAME, "title",
                "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString(),
                "analyzer_class", "org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer",
                "tokenization_enable_stemming", "true",
                "tokenization_locale", "en",
                "tokenization_skip_stop_words", "true",
                "tokenization_normalize_lowercase", "true")));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_artist", IndexMetadata.Kind.CUSTOM, MapsFactory.mapFromArray(
                IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName(),
                IndexTarget.TARGET_OPTION_NAME, "artist",
                "mode", OnDiskIndexBuilder.Mode.CONTAINS.toString(),
                "analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer",
                "case_sensitive", "false")));

        return builder.indexes(indexes.build());
    }

    public static CompressionParams getCompressionParameters()
    {
        return getCompressionParameters(null);
    }

    public static CompressionParams getCompressionParameters(Integer chunkSize)
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.test.encryption", "false")))
        {
            final CompressionParams encryption = CompressionParams.forSystemTables(); // ignoring chunk size
            assert encryption.isEnabled() && encryption.getOtherOptions().containsKey(CompressionParams.CIPHER_ALGORITHM)
                    : "Please set system_info_encryption in the yaml to use encrypted tests.";
            return encryption;
        }
        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            return chunkSize != null ? CompressionParams.snappy(chunkSize) : CompressionParams.snappy();

        return CompressionParams.noCompression();
    }

    public static void cleanupAndLeaveDirs() throws IOException
    {
        // We need to stop and unmap all CLS instances prior to cleanup() or we'll get failures on Windows.
        CommitLog.instance.stopUnsafe(true);
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.restartUnsafe();
    }

    public static void cleanup()
    {
        // clean up commitlog
        FileUtils.deleteContent(DatabaseDescriptor.getCommitLogLocation());

        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (Path dir : DatabaseDescriptor.getAllDataFileLocations())
            FileUtils.deleteContent(dir);
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    public static void insertData(String keyspace, String columnFamily, int offset, int numberOfRows)
    {
        TableMetadata cfm = SchemaManager.instance.getTableMetadata(keyspace, columnFamily);

        for (int i = offset; i < offset + numberOfRows; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfm, ApolloTime.systemClockMicros(), ByteBufferUtil.bytes("key"+i));
            if (cfm.clusteringColumns() != null && !cfm.clusteringColumns().isEmpty())
                builder.clustering(ByteBufferUtil.bytes("col"+ i)).add("val", ByteBufferUtil.bytes("val" + i));
            else
                builder.add("val", ByteBufferUtil.bytes("val"+i));
            builder.build().apply();
        }
    }


    public static void cleanupSavedCaches()
    {
        File cachesDir = DatabaseDescriptor.getSavedCachesLocation();

        if (!cachesDir.exists() || !cachesDir.isDirectory())
            return;

        FileUtils.delete(cachesDir.listFiles());
    }

    public static void doSchemaChanges(SchemaTransformation... transformations)
    {
        doSchemaChanges(Arrays.asList(transformations));
    }


    /**
     Apply the provided schema transformations (in the list order).
     *
     @param transformations the transformation to apply.
     */
    public static void doSchemaChanges(List<SchemaTransformation> transformations)
    {
        try
        {
            SchemaManager.instance.apply(SchemaTransformations.batch(transformations)).get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(); // Shouldn't happen really
        }
        catch (ExecutionException e)
        {
            throw Throwables.cleaned(e);
        }
    }
}