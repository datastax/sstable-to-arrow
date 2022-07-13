/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.server.RMISocketFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.datastax.bdp.db.upgrade.ClusterVersionBarrierProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.reactivex.Single;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthCacheMBean;
import org.apache.cassandra.auth.AuthManager;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.PropertyConfiguration;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ThreadAwareSecurityManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStoreCQLHelper;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MapsFactory;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UnmodifiableArrayList;
import org.apache.cassandra.utils.time.ApolloTime;

import com.datastax.bdp.db.nodes.Nodes;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base class for CQL tests.
 *
 * TODO: this is now used as a test utility outside of CQL tests, we should really refactor it into a utility class
 * for things like setting up the network, inserting data, etc.
 */
public abstract class CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(CQLTester.class);

    public static final String KEYSPACE = "cql_test_keyspace";
    public static final String KEYSPACE_PER_TEST = "cql_test_keyspace_alt";
    protected static final boolean USE_PREPARED_VALUES = Boolean.valueOf(System.getProperty("cassandra.test.use_prepared", "true"));
    protected static final boolean REUSE_PREPARED = Boolean.valueOf(System.getProperty("cassandra.test.reuse_prepared", "true"));
    protected static final long ROW_CACHE_SIZE_IN_MB = Integer.valueOf(System.getProperty("cassandra.test.row_cache_size_in_mb", "0"));
    private static final AtomicInteger seqNumber = new AtomicInteger();
    public static final String DATA_CENTER = "datacenter1";
    public static final String RACK1 = "rack1";
    private static final User SUPER_USER = new User("cassandra", "cassandra");

    private static NativeTransportService server;
    private static JMXConnectorServer jmxServer;
    private static String jmxHost;
    private static int jmxPort;
    protected static MBeanServerConnection jmxConnection;

    private static Randomization random;

    protected static int nativePort;
    protected static InetAddress nativeAddr;
    private static final Map<Pair<User, ProtocolVersion>, Cluster> clusters = MapsFactory.newMap();
    private static final Map<Pair<User, ProtocolVersion>, Session> sessions = MapsFactory.newMap();
    private static boolean initialized;

    protected static final ThreadPoolExecutor schemaCleanup =
            new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    private enum ServerStatus
    {
        NONE,
        PENDING,
        INITIALIZED,
        FAILED
    }

    private static AtomicReference<ServerStatus> serverStatus = new AtomicReference<>(ServerStatus.NONE);
    private static CountDownLatch serverReady = new CountDownLatch(1);

    public static final List<ProtocolVersion> PROTOCOL_VERSIONS = new ArrayList<>();

    private static final String CREATE_INDEX_NAME_REGEX = "(\\s*(\\w*|\"\\w*\")\\s*)";
    private static final String CREATE_INDEX_REGEX = String.format("\\A\\s*CREATE(?:\\s+CUSTOM)?\\s+INDEX" +
                    "(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s*" +
                    "%s?\\s*ON\\s+(%<s\\.)?%<s\\s*" +
                    "(\\((?:\\s*\\w+\\s*\\()?%<s\\))?",
            CREATE_INDEX_NAME_REGEX);
    private static final Pattern CREATE_INDEX_PATTERN = Pattern.compile(CREATE_INDEX_REGEX, Pattern.CASE_INSENSITIVE);

    /** Return the current server version if supported by the driver, else
     * the latest that is supported.
     *
     * @return - the preferred versions that is also supported by the driver
     */
    public static final ProtocolVersion getDefaultVersion()
    {
        return PROTOCOL_VERSIONS.contains(ProtocolVersion.CURRENT)
                ? ProtocolVersion.CURRENT
                : PROTOCOL_VERSIONS.get(PROTOCOL_VERSIONS.size() - 1);
    }

    // Note: Using a method to construct the rule avoids problems with CQLTester-using JMH benchmarks.
    @Rule
    public FailureWatcher failureRule()
    {
        return new FailureWatcher();
    }

    @BeforeClass
    public static void setupCQLTester()
    {
        initialized = true;

        // The latest versions might not be supported yet by the java driver
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
        {
            try
            {
                com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt());
                PROTOCOL_VERSIONS.add(version);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Protocol Version {} not supported by java driver", version);
            }
        }

        // Register an EndpointSnitch which returns fixed values for test.
        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override public String getRack(InetAddress endpoint) { return RACK1; }
            @Override public String getDatacenter(InetAddress endpoint) { return DATA_CENTER; }
            @Override public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) { return 0; }
        });

        // call after setting the snitch so that DD.applySnitch will not set the snitch to the conf snitch. On slow
        // Jenkins machines, DynamicEndpointSnitch::updateScores may initialize SS with a wrong TMD before we have a
        // chance to change the partitioner, so changing the DD snitch is not enough, we must prevent the config snitch
        // from being initialized.
        DatabaseDescriptor.daemonInitialization();

        nativeAddr = DatabaseDescriptor.getNativeTransportAddress();
        nativePort = DatabaseDescriptor.getNativeTransportPort();
    }

    public static ResultMessage lastSchemaChangeResult;

    private List<String> keyspaces = new ArrayList<>();
    private List<String> tables = new ArrayList<>();
    private List<String> views = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    private List<String> functions = new ArrayList<>();
    private List<String> aggregates = new ArrayList<>();
    private User user;

    // We don't use USE_PREPARED_VALUES in the code below so some test can foce value preparation (if the result
    // is not expected to be the same without preparation)
    private boolean usePrepared = USE_PREPARED_VALUES;
    private static boolean reusePrepared = REUSE_PREPARED;

    public static Runnable preJoinHook = () -> {};

    protected boolean usePrepared()
    {
        return usePrepared;
    }

    public static void prepareServer()
    {
        prepareServer(true);
    }

    public static void prepareServer(boolean restartCommitLog)
    {
        if (serverStatus.get() == ServerStatus.INITIALIZED)
            return;

        if (!initialized)
            setupCQLTester();

        if (!serverStatus.compareAndSet(ServerStatus.NONE, ServerStatus.PENDING))
        {   // Once per-JVM is enough, the first test to execute gets to initialize the server,
            // unless sub-classes call this method from static initialization methods such as
            // requireNetwork(). Note that this method cannot be called by the base class setUp
            // static method because this would make it impossible to change the partitioner in
            // sub-classed. Normally we run tests sequentially but, to be safe, this ensures
            // that if 2 tests execute in parallel, only one test initializes the server and the
            // other test waits.

            // if we couldn't set the server status to pending because another test previously failed
            // then abort without waiting
            if (serverStatus.get() == ServerStatus.FAILED)
                fail("A previous test failed to initialize the server");

            // if we've raced with another test then wait for a sufficient amount of time (Jenkins is quite slow)
            Uninterruptibles.awaitUninterruptibly(serverReady, 3, TimeUnit.MINUTES);

            // now check again if the test that ran in parallel failed
            if (serverStatus.get() == ServerStatus.FAILED)
                fail("A previous test failed to initialize the server: " + serverStatus);

            return;
        }

        try
        {
            // Not really required, but doesn't hurt either
            TPC.ensureInitialized(false);

            // Cleanup first
            try
            {
                cleanupAndLeaveDirs(restartCommitLog);
            }
            catch (IOException e)
            {
                logger.error("Failed to cleanup and recreate directories.");
                throw new RuntimeException(e);
            }

            // Some tests, e.g. OutOfSpaceTest, require the default handler installed by CassandraDaemon.
            if (Thread.getDefaultUncaughtExceptionHandler() == null)
                Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("Fatal exception in thread " + t, e));

            ThreadAwareSecurityManager.install();

            Keyspace.setInitialized();
            Nodes.Instance.persistLocalMetadata();

            SchemaKeyspace.saveSystemKeyspacesSchema();
            Nodes.Instance.persistLocalMetadata();

            StorageService.instance.populateTokenMetadata();

            preJoinHook.run();

            //TPC requires local vnodes to be generated so we need to
            //put the SS through join.
            StorageService.instance.initServer();
            if (StorageService.instance.getLocalHostUUID() == null)
            {
                StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), FBUtilities.getBroadcastAddress());
            }

            // clear the SS shutdown hook as it makes the test take longer on Jenkins
            JVMStabilityInspector.removeShutdownHooks();

            Gossiper.instance.registerUpgradeBarrierListener();

            logger.info("Server initialized");
            serverStatus.set(ServerStatus.INITIALIZED);
        }
        catch (Throwable t)
        {
            logger.error("Failed to initialize the server: {}", t.getMessage(), t);
            serverStatus.set(ServerStatus.FAILED);
            throw t;
        }
        finally
        {
            // signal to any other waiting test that the server is ready
            serverReady.countDown();
        }
    }

    /**
     * Starts the JMX server. It's safe to call this method multiple times.
     */
    public static void startJMXServer() throws Exception
    {
        if (jmxServer != null)
            return;

        InetAddress loopback = InetAddress.getLoopbackAddress();
        jmxHost = loopback.getHostAddress();
        try (ServerSocket sock = new ServerSocket())
        {
            sock.bind(new InetSocketAddress(loopback, 0));
            jmxPort = sock.getLocalPort();
        }

        jmxServer = JMXServerUtils.createJMXServer(jmxPort, true);
        jmxServer.start();
    }

    public static JMXServiceURL getJMXServiceURL() throws MalformedURLException
    {
        assert jmxServer != null : "jmxServer not started";

        return new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", jmxHost, jmxPort));
    }

    public static void createMBeanServerConnection() throws Exception
    {
        assert jmxServer != null : "jmxServer not started";

        Map<String, Object> env = MapsFactory.newMap();
        env.put("com.sun.jndi.rmi.factory.socket", RMISocketFactory.getDefaultSocketFactory());
        JMXConnector jmxc = JMXConnectorFactory.connect(getJMXServiceURL(), env);
        jmxConnection =  jmxc.getMBeanServerConnection();
    }

    public static Randomization getRandom()
    {
        if (random == null)
            random = new Randomization();
        return random;
    }

    public static void cleanupAndLeaveDirs(boolean restartCommitLog) throws IOException
    {
        // We need to stop and unmap all CLS instances prior to cleanup() or we'll get failures on Windows.
        if (restartCommitLog)
            CommitLog.instance.stopUnsafe(true);

        mkdirs();
        cleanup();
        mkdirs();

        if (restartCommitLog)
            CommitLog.instance.restartUnsafe();
    }

    public static void cleanup()
    {
        // clean up commitlog
        FileUtils.deleteContent(DatabaseDescriptor.getCommitLogLocation());

        File cdcDir = DatabaseDescriptor.getCDCLogLocation();
        if (cdcDir != null && cdcDir.exists())
            FileUtils.deleteRecursive(cdcDir);

        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (Path dir : DatabaseDescriptor.getAllDataFileLocations())
            FileUtils.deleteContent(dir);
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    public static void cleanupSavedCaches()
    {
        File cachesDir = DatabaseDescriptor.getSavedCachesLocation();

        if (!cachesDir.exists() || !cachesDir.isDirectory())
            return;

        FileUtils.delete(cachesDir.listFiles());
    }

    @BeforeClass
    public static void setUpClass()
    {
        if (ROW_CACHE_SIZE_IN_MB > 0)
            DatabaseDescriptor.setRowCacheSizeInMB(ROW_CACHE_SIZE_IN_MB);

        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @AfterClass
    public static void tearDownClass()
    {
        for (Session sess : sessions.values())
            sess.close();

        // Close driver instances concurrently to speed up test tear down
        if (!clusters.isEmpty())
        {
            ExecutorService pool = Executors.newFixedThreadPool(clusters.size());
            List<Future<?>> async = new ArrayList<>();
            for (Cluster cl : clusters.values())
                async.add(pool.submit(cl::close));
            for (Future<?> future : async)
            {
                try
                {
                    future.get(30, TimeUnit.SECONDS);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
            pool.shutdown();
        }

        if (server != null)
            server.stop();

        // We use queryInternal for CQLTester so prepared statement will populate our internal cache (if reusePrepared is used; otherwise prepared
        // statements are not cached but re-prepared every time). So we clear the cache between test files to avoid accumulating too much.
        if (reusePrepared)
            QueryProcessor.clearInternalStatementsCache();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        // this call is idempotent and will only prepare the server on the first call
        // we cannot prepare the server in setUpClass() because otherwise it would
        // be impossible to change the partitioner in sub-classes, e.g. SelectOrderedPartitionerTest
        prepareServer();

        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE_PER_TEST));
    }

    @After
    public void afterTest() throws Throwable
    {
        onStartingAfterTest();

        dropPerTestKeyspace();

        // Restore standard behavior in case it was changed
        usePrepared = USE_PREPARED_VALUES;
        reusePrepared = REUSE_PREPARED;

        final List<String> keyspacesToDrop = copy(keyspaces);
        final List<String> tablesToDrop = copy(tables);
        final List<String> viewsToDrop = copy(views);
        final List<String> typesToDrop = copy(types);
        final List<String> functionsToDrop = copy(functions);
        final List<String> aggregatesToDrop = copy(aggregates);
        keyspaces = null;
        tables = null;
        views = null;
        types = null;
        functions = null;
        aggregates = null;
        user = null;

        // We want to clean up after the test, but dropping a table is rather long so just do that asynchronously
        schemaCleanup.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.debug("Dropping {} materialized view created in previous test", viewsToDrop.size());
                    for (int i = viewsToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEYSPACE, viewsToDrop.get(i)));

                    logger.debug("Dropping {} tables created in previous test", tablesToDrop.size());
                    for (int i = tablesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tablesToDrop.get(i)));

                    logger.debug("Dropping {} aggregate functions created in previous test", aggregatesToDrop.size());
                    for (int i = aggregatesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP AGGREGATE IF EXISTS %s", aggregatesToDrop.get(i)));

                    logger.debug("Dropping {} scalar functions created in previous test", functionsToDrop.size());
                    for (int i = functionsToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP FUNCTION IF EXISTS %s", functionsToDrop.get(i)));

                    logger.debug("Dropping {} types created in previous test", typesToDrop.size());
                    for (int i = typesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP TYPE IF EXISTS %s.%s", KEYSPACE, typesToDrop.get(i)));

                    logger.debug("Dropping {} keyspaces created in previous test", keyspacesToDrop.size());
                    for (int i = keyspacesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP KEYSPACE IF EXISTS %s", keyspacesToDrop.get(i)));

                    // Dropping doesn't delete the sstables. It's not a huge deal but it's cleaner to cleanup after us
                    // Thas said, we shouldn't delete blindly before the TransactionLogs.SSTableTidier for the table we drop
                    // have run or they will be unhappy. Since those taks are scheduled on StorageService.tasks and that's
                    // mono-threaded, just push a task on the queue to find when it's empty. No perfect but good enough.

                    logger.debug("Awaiting sstable cleanup");
                    final CountDownLatch latch = new CountDownLatch(1);
                    ScheduledExecutors.nonPeriodicTasks.execute(new Runnable()
                    {
                        public void run()
                        {
                            latch.countDown();
                        }
                    });
                    if (!latch.await(2, TimeUnit.SECONDS))
                        logger.warn("Times out waiting for shutdown");

                    logger.debug("Removing sstables");
                    removeAllSSTables(KEYSPACE, tablesToDrop);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * A method called just at the beginning of the {@link #afterTest()} method, that does nothing by default but that
     * tests can override if they want to make sure something runs _before_ the code from {@link #afterTest()}.
     *
     * <p>Note that while tests could use their own {@code @After} annotation, such method would run _after_ the
     * {@link #afterTest()} (that's how JUnit works, it calls the {@code @Before} and {@code @After} of the parents
     * before the childs), and that can be a problem (say, the tests create MVs and want to drop them after the tests:
     * if it puts it in an {@code @After} method, {@link #afterTest()} may end up trying to delete the tables first,
     * and may fail because a table has a non-yet-dropped MV that depend on it. Which can substantially break followup
     * tests).
     */
    protected void onStartingAfterTest() throws Throwable
    {
        // nothing by default
    }

    public static class ToolRunner implements AutoCloseable
    {
        private final List<String> allArgs = new ArrayList<>();
        private Process process;
        private final ByteArrayOutputStream errLineBuffer = new ByteArrayOutputStream();
        private final ByteArrayOutputStream outLineBuffer = new ByteArrayOutputStream();
        private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
        private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        private long defaultTimeoutMillis = TimeUnit.SECONDS.toMillis(30);
        private Thread ioWatcher;
        private InputStream stdin;
        private boolean stdinAutoClose;
        private Map<String, String> envs;

        public ToolRunner(List<String> args) {
            this.allArgs.addAll(args);
        }

        public ToolRunner withStdin(InputStream stdin, boolean autoClose)
        {
            this.stdin = stdin;
            this.stdinAutoClose = autoClose;
            return this;
        }

        public ToolRunner withEnvs(Map<String, String> envs)
        {
            this.envs = envs;
            return this;
        }

        public ToolRunner start()
        {
            if (process != null)
                throw new IllegalStateException("Process already started. Create a new ToolRunner instance for each invocation.");

            logger.debug("Starting {}", argsToLogString());

            try
            {
                ProcessBuilder pb = new ProcessBuilder(allArgs);
                if (envs != null)
                    pb.environment().putAll(envs);
                process = pb.start();
                ioWatcher = new Thread(this::watchIO);
                ioWatcher.setDaemon(true);
                ioWatcher.start();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to start " + allArgs, e);
            }

            return this;
        }

        private void watchIO()
        {
            OutputStream in = process.getOutputStream();
            InputStream err = process.getErrorStream();
            InputStream out = process.getInputStream();
            byte[] buf = new byte[1024];
            while (true)
            {
                processStdin(in, buf);
                boolean errHandled = processInput(err, buf, errLineBuffer, errBuffer, "stderr");
                boolean outHandled = processInput(out, buf, outLineBuffer, outBuffer, "stdout");
                if (!errHandled && !outHandled)
                {
                    if (!process.isAlive())
                        return;
                    try
                    {
                        Thread.sleep(50L);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        private void processStdin(OutputStream in, byte[] buf)
        {
            if (stdin == null)
                return;

            try
            {
                int avail = stdin.available();
                if (avail <= 0)
                {
                    if (stdinAutoClose)
                    {
                        in.close();
                        stdin = null;
                    }
                    return;
                }

                int rd = stdin.read(buf, 0, Math.min(avail, buf.length));
                in.write(buf, 0, rd);
            }
            catch (Exception e)
            {
                logger.error("Error writing to tool's stdin", e);
            }
        }

        private boolean processInput(InputStream input,
                                     byte[] buf,
                                     ByteArrayOutputStream lineBuffer,
                                     ByteArrayOutputStream buffer,
                                     String name)
        {
            try
            {
                int avail = input.available();
                if (avail <= 0)
                    return false;

                int rd = input.read(buf, 0, Math.min(avail, buf.length));

                for (int i = 0; i < rd; i++)
                {
                    byte b = buf[i];
                    if (b == 10)
                    {
                        logger.info("tool {}: {}", name, lineBuffer.toString());
                        lineBuffer.reset();
                    }
                    else
                    {
                        lineBuffer.write(((int) b) & 0xff);
                    }
                }
                buffer.write(buf, 0, rd);

                return true;
            }
            catch (Exception e)
            {
                logger.error("Error reading from tool's {}", name, e);

                return false;
            }
        }

        public boolean isRunning()
        {
            return process != null && process.isAlive();
        }

        public boolean waitFor()
        {
            return waitFor(defaultTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        public boolean waitFor(long time, TimeUnit timeUnit)
        {
            try
            {
                if (!process.waitFor(time, timeUnit))
                    return false;
                ioWatcher.join();
                return true;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public ToolRunner waitAndAssertOnExitCode()
        {
            assertTrue(String.format("Tool %s didn't terminate",
                            argsToLogString()),
                    waitFor());
            return assertOnExitCode();
        }

        public ToolRunner assertOnExitCode()
        {
            int code = getExitCode();
            if (code != 0)
                fail(String.format("%s%nexited with code %d%nstderr:%n%s%nstdout:%n%s",
                        argsToLogString(),
                        code,
                        getStderr(),
                        getStdout()));
            return this;
        }

        public String argsToLogString()
        {
            return allArgs.stream().collect(Collectors.joining(",\n    ", "[", "]"));
        }

        public int getExitCode()
        {
            return process.exitValue();
        }

        public String getStdout()
        {
            return outBuffer.toString();
        }

        public String getStderr()
        {
            return errBuffer.toString();
        }

        public void forceKill()
        {
            try
            {
                process.exitValue();
                // process no longer alive - just ignore that fact
            }
            catch (IllegalThreadStateException e)
            {
                process.destroyForcibly();
            }
        }

        @Override
        public void close()
        {
            forceKill();
        }
    }

    protected ToolRunner invokeNodetool(String... args)
    {
        return invokeNodetool(Arrays.asList(args));
    }

    protected ToolRunner invokeNodetool(List<String> args)
    {
        return invokeTool(buildNodetoolArgs(args));
    }

    private static List<String> buildNodetoolArgs(List<String> args)
    {
        List<String> allArgs = new ArrayList<>();
        allArgs.add(resolveFromDseDbRoot("bin/nodetool").toString());
        allArgs.add("-p");
        allArgs.add(Integer.toString(jmxPort));
        allArgs.add("-h");
        allArgs.add(jmxHost);
        allArgs.addAll(args);
        return allArgs;
    }

    /**
     * Resolves a path of a tool (e.g. {@code bin/nodetool}) relative to {@code dse-db/}, using
     * the system property {@code dse-db.root}, which is mandatory if running in CI.
     *
     * Note that {@code dse-db/lib} needs to be populated for the tools.
     *
     * If {@code tool} is already an absolute path, it will be returned as it is.
     */
    public static Path resolveFromDseDbRoot(String tool)
    {
        Path toolPath = Paths.get(tool);
        return toolPath.isAbsolute()
                ? toolPath
                : Paths.get(System.getProperty("dse-db.root",
                        new File(".").getAbsolutePath()))
                .resolve(toolPath);
    }

    protected ToolRunner invokeTool(String... args)
    {
        return invokeTool(Arrays.asList(args));
    }

    protected ToolRunner invokeTool(List<String> args)
    {
        ToolRunner runner = new ToolRunner(args);
        runner.start();
        return runner;
    }

    protected static void requireAuthentication()
    {
        System.setProperty("cassandra.superuser_setup_delay_ms", "-1");

        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setAuthManager(new AuthManager(new CassandraRoleManager(), new CassandraAuthorizer()));
    }

    public static void requireNetwork() throws ConfigurationException
    {
        if (server != null)
            return;

        prepareServer();

        SchemaKeyspace.saveSystemKeyspacesSchema();
        Nodes.Instance.persistLocalMetadata();
        StorageService.instance.populateTokenMetadata();
        StorageService.instance.initServer();
        Gossiper.instance.register(StorageService.instance);
        SchemaLoader.startGossiper();
        Gossiper.instance.maybeInitializeLocalState(1);

        Gossiper.instance.registerUpgradeBarrierListener();
        ClusterVersionBarrierProvider.instance.getBarrier().onLocalNodeReady();

        server = new NativeTransportService(nativeAddr, nativePort);
        server.start();
    }

    private static Cluster initClientCluster(User user, ProtocolVersion version)
    {
        Pair<User, ProtocolVersion> key = Pair.create(user, version);
        Cluster cluster = clusters.get(key);
        if (cluster != null)
            return cluster;

        Cluster.Builder builder = clusterBuilder(version);
        if (user != null)
            builder.withCredentials(user.username, user.password);
        cluster = builder.build();

        logger.info("Started Java Driver instance for protocol version {}", version);

        return cluster;
    }

    public static Cluster.Builder clusterBuilder()
    {
        return Cluster.builder()
                .addContactPoints(nativeAddr)
                .withPort(nativePort)
                .withClusterName("Test Cluster")
                .withNettyOptions(NettyOptions.DEFAULT_INSTANCE)
                .withoutMetrics()
                .withMonitorReporting(false);
    }

    public static Cluster.Builder clusterBuilder(ProtocolVersion version)
    {
        Cluster.Builder builder = clusterBuilder();
        if (version.isBeta())
            builder = builder.allowBetaProtocolVersion();
        else
            builder = builder.withProtocolVersion(com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt()));
        return builder;
    }

    public static Cluster createClientCluster(ProtocolVersion version, String clusterName, NettyOptions nettyOptions,
                                              String username, String password)
    {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(nativeAddr)
                .withClusterName(clusterName)
                .withPort(nativePort)
                .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt()))
                .withNettyOptions(nettyOptions)
                .withoutMetrics()
                .withMonitorReporting(false);
        if (username != null)
            builder.withAuthProvider(new PlainTextAuthProvider(username, password));
        return builder.build();
    }

    public static void closeClientCluster(Cluster cluster)
    {
        cluster.closeAsync().force();
        logger.info("Closed Java Driver instance for cluster {}", cluster.getClusterName());
    }

    protected void dropPerTestKeyspace() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_PER_TEST));
    }

    /**
     * Returns a copy of the specified list.
     * @return a copy of the specified list.
     */
    private static List<String> copy(List<String> list)
    {
        return list.isEmpty() ? Collections.<String>emptyList() : new ArrayList<>(list);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return getCurrentColumnFamilyStore(KEYSPACE);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore(String keyspace)
    {
        return getCurrentColumnFamilyStore(keyspace, currentTable());
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore(String keyspace, String table)
    {
        return table == null ? null : Keyspace.open(keyspace).getColumnFamilyStore(table);
    }

    public void flush(boolean forceFlush)
    {
        if (forceFlush)
            flush();
    }

    public void flush()
    {
        flush(KEYSPACE);
    }

    public void flush(String keyspace)
    {
        flush(keyspace, currentTable());
    }

    public void flush(String keyspace, String table)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace, table);
        if (store != null)
            store.forceBlockingFlush();
    }

    public void disableCompaction(String keyspace)
    {
        disableCompaction(keyspace, currentTable());
    }

    public void disableCompaction(String keyspace, String table)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace, table);
        if (store != null)
            store.disableAutoCompaction();
    }

    public void compact()
    {
        compact(KEYSPACE);
    }

    public void compact(String keyspace)
    {
        compact(keyspace, currentTable());
    }

    public void compact(String keyspace, String table)
    {
        try
        {
            ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace, table);
            if (store != null)
                store.forceMajorCompaction();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void disableCompaction()
    {
        disableCompaction(KEYSPACE);
    }

    public void enableCompaction(String keyspace)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
        if (store != null)
            store.enableAutoCompaction();
    }

    public void enableCompaction()
    {
        enableCompaction(KEYSPACE);
    }

    public void cleanupCache()
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        if (store != null)
            store.cleanupCache();
    }

    public static FunctionName parseFunctionName(String qualifiedName)
    {
        int i = qualifiedName.indexOf('.');
        return i == -1
                ? FunctionName.nativeFunction(qualifiedName)
                : new FunctionName(qualifiedName.substring(0, i).trim(), qualifiedName.substring(i+1).trim());
    }

    public static String shortFunctionName(String f)
    {
        return parseFunctionName(f).name;
    }

    private static void removeAllSSTables(String ks, List<String> tables)
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (Path d : Directories.getKSChildDirectories(ks))
        {
            if (Files.exists(d) && containsAny(d.getFileName().toString(), tables))
                FileUtils.deleteRecursive(d);
        }
    }

    private static boolean containsAny(String filename, List<String> tables)
    {
        for (int i = 0, m = tables.size(); i < m; i++)
            // don't accidentally delete in-use directories with the
            // same prefix as a table to delete, i.e. table_1 & table_11
            if (filename.contains(tables.get(i) + "-"))
                return true;
        return false;
    }

    protected String keyspace()
    {
        return KEYSPACE;
    }

    protected String currentTable()
    {
        if (tables.isEmpty())
            return null;
        return tables.get(tables.size() - 1);
    }

    protected String currentView()
    {
        if (views.isEmpty())
            return null;
        return views.get(views.size() - 1);
    }

    protected Collection<String> currentTables()
    {
        if (tables == null || tables.isEmpty())
            return UnmodifiableArrayList.emptyList();

        return new ArrayList<>(tables);
    }

    protected ByteBuffer unset()
    {
        return ByteBufferUtil.UNSET_BYTE_BUFFER;
    }

    protected void forcePreparedValues()
    {
        this.usePrepared = true;
    }

    protected void stopForcingPreparedValues()
    {
        this.usePrepared = USE_PREPARED_VALUES;
    }

    protected void disablePreparedReuseForTest()
    {
        this.reusePrepared = false;
    }

    protected String createType(String keyspace, String query)
    {
        String typeName = createTypeName();
        String fullQuery = String.format(query, keyspace + "." + typeName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return typeName;
    }

    protected String createTypeName()
    {
        String typeName = "type_" + seqNumber.getAndIncrement();
        types.add(typeName);
        return typeName;
    }

    protected String createType(String query)
    {
        return createType(KEYSPACE, query);
    }

    protected String createFunction(String keyspace, String argTypes, String query) throws Throwable
    {
        String functionName = keyspace + ".function_" + seqNumber.getAndIncrement();
        createFunctionOverload(functionName, argTypes, query);
        return functionName;
    }

    protected void createFunctionOverload(String functionName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, functionName);
        functions.add(functionName + '(' + argTypes + ')');
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createAggregate(String keyspace, String argTypes, String query) throws Throwable
    {
        String aggregateName = keyspace + "." + "aggregate_" + seqNumber.getAndIncrement();
        createAggregateOverload(aggregateName, argTypes, query);
        return aggregateName;
    }

    protected void createAggregateOverload(String aggregateName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, aggregateName);
        aggregates.add(aggregateName + '(' + argTypes + ')');
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createKeyspace(String query)
    {
        String currentKeyspace = createKeyspaceName();
        String fullQuery = String.format(query, currentKeyspace);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentKeyspace;
    }

    protected String createKeyspaceName()
    {
        String currentKeyspace = "keyspace_" + seqNumber.getAndIncrement();
        keyspaces.add(currentKeyspace);
        return currentKeyspace;
    }

    public String createTable(String query)
    {
        return createTable(KEYSPACE, query);
    }

    public String createTable(String keyspace, String query)
    {
        return createTable(keyspace, query, true);
    }

    public String createTable(String keyspace, String query, boolean withCompression)
    {
        String currentTable = createTableName();
        String fullQuery = formatQuery(keyspace, query);
        fullQuery = withCompression ? addCompression(fullQuery) : fullQuery;
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentTable;
    }

    public String createView(String query)
    {
        return createViewWithName(null, query);
    }

    public String createViewWithName(String viewName, String query)
    {
        String currentView = viewName == null ? createViewName() : viewName;
        String fullQuery = String.format(query, KEYSPACE + "." + currentView, KEYSPACE + "." + currentTable());
        fullQuery = addCompression(fullQuery);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentView;
    }

    public void dropView(String view)
    {
        dropFormattedTable(String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEYSPACE, view));
        views.remove(view);
    }

    /**
     * Add compression parameters according to the test options (cassandra.test.compression and cassandra.test.encryption).
     * Also explicitly disables compression if the options are not present.
     * Pattern tested by org.apache.cassandra.cql3.CQLSyntaxHelperTest#testAddCompression().
     */
    static String addCompression(String query)
    {
        // Split the query in parts before and after WITH and the trailing semicolon. Also recognize "WITH/AND compression"
        Pattern pattern = Pattern.compile("([^\"]*?(\"[^\"]*\"[^\"]*?)*)([ \t)}]with .*?((and )?compression.*)?)?(;.*)?",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(query);
        if (!matcher.matches())
            return query;   // unexpected, leave as is
        if (!matcher.matches() || matcher.group(4) != null)
            return query;   // already specifies compression, leave as is

        CompressionParams params = SchemaLoader.getCompressionParameters();
        return String.format("%s%s compression = %s%s",
                matcher.group(1),
                matcher.group(3) != null ? matcher.group(3) + " and" : " with",
                ColumnFamilyStoreCQLHelper.toCQL(params.asMap()),
                matcher.group(6) != null ? matcher.group(6) : "");
    }

    protected String createTableName()
    {
        String currentTable = "table_" + seqNumber.getAndIncrement();
        tables.add(currentTable);
        return currentTable;
    }

    protected String createViewName()
    {
        String currentView = "view_" + seqNumber.getAndIncrement();
        views.add(currentView);
        return currentView;
    }

    protected String createTableMayThrow(String query)
    {
        String currentTable = createTableName();
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery).blockingGet();
        return currentTable;
    }

    protected void alterTable(String query)
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void alterTableMayThrow(String query)
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery).blockingGet();
    }

    public void dropTable(String query)
    {
        dropTable(KEYSPACE, query);
    }

    public void dropTable(String keyspace, String query)
    {
        dropFormattedTable(String.format(query, keyspace + "." + currentTable()));
    }

    protected void dropFormattedTable(String formattedQuery)
    {
        logger.info(formattedQuery);
        schemaChange(formattedQuery);
    }

    protected String createIndex(String query)
    {
        String formattedQuery = formatQuery(query);
        return createFormattedIndex(formattedQuery);
    }

    protected String createFormattedIndex(String formattedQuery)
    {
        logger.info(formattedQuery);
        String indexName = getCreateIndexName(formattedQuery);
        schemaChange(formattedQuery);
        return indexName;
    }

    protected static String getCreateIndexName(String formattedQuery)
    {
        Matcher matcher = CREATE_INDEX_PATTERN.matcher(formattedQuery);
        if (!matcher.find())
            throw new IllegalArgumentException("Expected valid create index query but found: " + formattedQuery);

        String index = matcher.group(2);
        if (!Strings.isNullOrEmpty(index))
            return index;

        String keyspace = matcher.group(5);
        if (Strings.isNullOrEmpty(keyspace))
            throw new IllegalArgumentException("Keyspace name should be specified: " + formattedQuery);

        String table = matcher.group(7);
        if (Strings.isNullOrEmpty(table))
            throw new IllegalArgumentException("Table name should be specified: " + formattedQuery);

        String column = matcher.group(9);

        String baseName = Strings.isNullOrEmpty(column)
                ? IndexMetadata.generateDefaultIndexName(table)
                : IndexMetadata.generateDefaultIndexName(table, new ColumnIdentifier(column, true));

        KeyspaceMetadata ks = SchemaManager.instance.getKeyspaceMetadata(keyspace);
        return ks.findAvailableIndexName(baseName);
    }

    /**
     * Index creation is asynchronous, this method searches in the system table IndexInfo
     * for the specified index and returns true if it finds it, which indicates the
     * index was built. If we haven't found it after 5 seconds we give-up.
     */
    protected boolean waitForIndex(String keyspace, String index) throws Throwable
    {
        long start = ApolloTime.millisSinceStartup();
        boolean indexCreated = false;
        while (!indexCreated)
        {
            // Note: The table_name in IndexInfo here is actually the name of the keyspace :(
            Object[][] results = getRows(execute("select index_name from system.\"IndexInfo\" where table_name = ? AND index_name = ?", keyspace, index));
            for(int i = 0; i < results.length; i++)
            {
                if (index.equals(results[i][0]))
                {
                    indexCreated = true;
                    break;
                }
            }

            if (ApolloTime.millisSinceStartupDelta(start) > 5000)
                break;

            Thread.sleep(10);
        }

        return indexCreated;
    }

    /**
     * This method waits for the index to be queryable after creation.
     */
    protected boolean waitForIndexQueryable(String index) throws InterruptedException
    {
        long start = ApolloTime.millisSinceStartup();
        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore(KEYSPACE).indexManager;

        while (true)
        {
            if (indexManager.isIndexQueryable(index))
            {
                return true;
            }
            else if (ApolloTime.millisSinceStartupDelta(start) > 5000)
            {
                return false;
            }
            else
            {
                Thread.sleep(10);
            }
        }
    }

    /**
     * Index creation is asynchronous, this method waits until the specified index hasn't any building task running.
     * <p>
     * This method differs from {@link #waitForIndex(String, String)} in that it doesn't require the index to be
     * fully nor successfully built, so it can be used to wait for failing index builds.
     *
     * @param keyspace the index keyspace name
     * @param indexName the index name
     * @return {@code true} if the index build tasks have finished in 5 seconds, {@code false} otherwise
     */
    protected boolean waitForIndexBuilds(String keyspace, String indexName) throws InterruptedException
    {
        long start = ApolloTime.millisSinceStartup();
        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore(keyspace).indexManager;

        while (true)
        {
            if (!indexManager.isIndexBuilding(indexName))
            {
                return true;
            }
            else if (ApolloTime.millisSinceStartupDelta(start) > 5000)
            {
                return false;
            }
            else
            {
                Thread.sleep(10);
            }
        }
    }

    protected void createIndexMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery).blockingGet();
    }

    protected void dropIndex(String query) throws Throwable
    {
        String fullQuery = String.format(query, KEYSPACE);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected static boolean isViewBuiltBlocking(String keyspaceName, String viewName)
    {
        return TPCUtils.blockingGet(SystemKeyspace.isViewBuilt(keyspaceName, viewName));
    }

    /**
     *  Because the tracing executor is single threaded, submitting an empty event should ensure
     *  that all tracing events mutations have been applied.
     */
    protected void waitForTracingEvents()
    {
        try
        {
            StageManager.tracingExecutor.submit(() -> {}).get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to wait for tracing events: {}", t);
        }
    }

    protected void assertLastSchemaChange(Event.SchemaChange.Change change, Event.SchemaChange.Target target,
                                          String keyspace, String name,
                                          String... argTypes)
    {
        assertTrue(lastSchemaChangeResult instanceof ResultMessage.SchemaChange);
        ResultMessage.SchemaChange schemaChange = (ResultMessage.SchemaChange) lastSchemaChangeResult;
        Assert.assertSame(change, schemaChange.change.change);
        Assert.assertSame(target, schemaChange.change.target);
        Assert.assertEquals(keyspace, schemaChange.change.keyspace);
        Assert.assertEquals(name, schemaChange.change.name);
        Assert.assertEquals(argTypes != null ? Arrays.asList(argTypes) : null, schemaChange.change.argTypes);
    }

    protected static void schemaChange(String query)
    {
        try
        {
            logger.info("SCHEMA CHANGE: " + query);

            ClientState state = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            QueryState queryState = new QueryState(state, UserRolesAndPermissions.SYSTEM);

            CQLStatement statement = QueryProcessor.parseStatement(query, queryState);
            statement.validate(queryState);

            QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());

            lastSchemaChangeResult = statement.executeLocally(queryState, options).blockingGet();
        }
        catch (RequestValidationException e)
        {
            // Getting a validation exception is kind of a "normal" outcome if the query is invalid in the first place,
            // which a number of tests are testing. Let's not wrap this any further.
            throw e;
        }
        catch (Exception e)
        {
            logger.info("Error performing schema change", e);
            throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
        }
    }

    protected TableMetadata currentTableMetadata()
    {
        return currentTableMetadata(KEYSPACE);
    }

    protected TableMetadata currentTableMetadata(String keyspace)
    {
        return tableMetadata(keyspace, currentTable());
    }

    protected TableMetadata tableMetadata(String ks, String name)
    {
        assert tables.contains(name);
        return SchemaManager.instance.getTableMetadata(ks, name);
    }

    protected com.datastax.driver.core.ResultSet executeNet(ProtocolVersion protocolVersion, String query, Object... values)
    {
        return sessionNet(protocolVersion).execute(formatQuery(query), values);
    }

    protected com.datastax.driver.core.ResultSet executeNetAs(String proxyUser, String query, Object... values)
    {
        SimpleStatement statement = new SimpleStatement(formatQuery(query), values);
        statement.executingAs(proxyUser);
        return sessionNet(getDefaultVersion()).execute(statement);
    }

    protected com.datastax.driver.core.ResultSet executeNet(String query, Object... values) throws Throwable
    {
        return executeNet(getDefaultVersion(), query, values);
    }

    protected com.datastax.driver.core.PreparedStatement prepareNet(ProtocolVersion protocolVersion, String query)
    {
        return sessionNet(protocolVersion).prepare(query);
    }

    protected com.datastax.driver.core.PreparedStatement prepareNet(String query)
    {
        return prepareNet(getDefaultVersion(), query);
    }

    protected com.datastax.driver.core.ResultSet executeNet(ProtocolVersion protocolVersion, Statement statement)
    {
        return sessionNet(protocolVersion).execute(statement);
    }

    protected com.datastax.driver.core.ResultSet executeNet(Statement statement)
    {
        return executeNet(getDefaultVersion(), statement);
    }

    protected com.datastax.driver.core.ResultSetFuture executeNetAsync(ProtocolVersion protocolVersion, Statement statement)
    {
        return sessionNet(protocolVersion).executeAsync(statement);
    }

    protected com.datastax.driver.core.ResultSet executeNetWithPaging(String query, int pageSize) throws Throwable
    {
        return sessionNet().execute(new SimpleStatement(formatQuery(query)).setFetchSize(pageSize));
    }

    /**
     * Use the specified user for executing the queries over the network.
     * @param username the user name
     * @param password the user password
     */
    public void useUser(String username, String password)
    {
        this.user = new User(username, password);
    }

    /**
     * Use the super user for executing the queries over the network.
     */
    public void useSuperUser()
    {
        this.user = SUPER_USER;
    }

    public boolean isSuperUser()
    {
        return SUPER_USER.equals(user);
    }

    public Session sessionNet()
    {
        return sessionNet(getDefaultVersion());
    }

    public Session sessionNet(ProtocolVersion protocolVersion)
    {
        requireNetwork();

        return getSession(protocolVersion);
    }

    protected SimpleClient newSimpleClient(ProtocolVersion version, boolean compression) throws IOException
    {
        return new SimpleClient(nativeAddr.getHostAddress(), nativePort, version, version.isBeta(), EncryptionOptions.ClientEncryptionOptions.newDefaultInstance()).connect(compression);
    }

    private Session getSession(ProtocolVersion protocolVersion)
    {
        Cluster cluster = getCluster(protocolVersion);
        return sessions.computeIfAbsent(Pair.create(user, protocolVersion), userProto -> cluster.connect());
    }

    private Cluster getCluster(ProtocolVersion protocolVersion)
    {
        return clusters.computeIfAbsent(Pair.create(user, protocolVersion),
                userProto -> initClientCluster(user, protocolVersion));
    }

    public static void invalidateAuthCaches()
    {
        invalidate("PermissionsCache");
        invalidate("RolesCache");
    }

    private static void invalidate(String authCacheName)
    {
        try
        {
            final ObjectName objectName = new ObjectName("org.apache.cassandra.auth:type=" + authCacheName);
            JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), objectName, AuthCacheMBean.class)
                    .invalidate();
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Cannot invalidate " + authCacheName, e);
        }
    }

    public String formatQuery(String query)
    {
        return formatQuery(KEYSPACE, query);
    }

    public String formatQuery(String keyspace, String query)
    {
        String currentTable = currentTable();
        return currentTable == null ? query : String.format(query, keyspace + "." + currentTable);
    }

    public String formatViewQuery(String keyspace, String query)
    {
        String currentView = currentView();
        return currentView == null ? query : String.format(query, keyspace + "." + currentView);
    }

    protected ResultMessage.Prepared prepare(String query) throws Throwable
    {
        return QueryProcessor.prepare(formatQuery(query), QueryState.forInternalCalls()).blockingGet();
    }

    public UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(query), values);
    }

    public UntypedResultSet executeWithKeyspace(String keyspace, String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(keyspace, query), values);
    }

    public UntypedResultSet executeWithoutAuthorization(String query, Object... values) throws Throwable
    {
        return executeFormattedQueryWithoutAuthorization(formatQuery(query), values);
    }

    public UntypedResultSet executeView(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatViewQuery(KEYSPACE, query), values);
    }

    public Single<UntypedResultSet> executeAsync(String query, Object... values) throws Throwable
    {
        return executeFormattedQueryAsync(formatQuery(query), values);
    }

    private Single<UntypedResultSet> executeFormattedQueryAsync(String query, Object... values) throws Throwable
    {
        return executeFormattedQueryAsync(true, query, values);
    }

    private Single<UntypedResultSet> executeFormattedQueryAsync(boolean checkAuth,
                                                                String query,
                                                                Object... values) throws Throwable
    {
        Single<UntypedResultSet> rs;
        if (usePrepared)
        {
            if (logger.isTraceEnabled())
                logger.trace("Executing: {} with values {}", query, formatAllValues(values));
            if (reusePrepared)
            {
                rs = QueryProcessor.executeInternalAsync(checkAuth, query, transformValues(values));

                // If a test uses a "USE ...", then presumably its statements use relative table. In that case, a USE
                // change the meaning of the current keyspace, so we don't want a following statement to reuse a previously
                // prepared statement at this wouldn't use the right keyspace. To avoid that, we drop the previously
                // prepared statement.
                if (query.startsWith("USE"))
                    QueryProcessor.clearInternalStatementsCache();
            }
            else
            {
                rs = QueryProcessor.executeOnceInternal(query, transformValues(values));
            }
        }
        else
        {
            query = replaceValues(query, values);
            if (logger.isTraceEnabled())
                logger.trace("Executing: {}", query);
            rs = QueryProcessor.executeOnceInternal(query);
        }
        return rs;
    }

    private UntypedResultSet executeFormattedQueryWithoutAuthorization(String query, Object... values) throws Throwable
    {
        UntypedResultSet rs = executeFormattedQueryAsync(false, query, values).blockingGet();
        if (rs != null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Got {} rows", rs.size());
        }
        return rs;
    }

    public UntypedResultSet executeFormattedQuery(String query, Object... values) throws Throwable
    {
        UntypedResultSet rs = executeFormattedQueryAsync(query, values).blockingGet();
        if (rs != null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Got {} rows", rs.size());
        }
        return rs;
    }

    protected void assertRowsNet(ResultSet result, Object[]... rows)
    {
        assertRowsNet(getDefaultVersion(), result, rows);
    }

    public static Comparator<List<ByteBuffer>> RowComparator = (Comparator<List<ByteBuffer>>) (row1, row2) -> {
        int ret = Integer.compare(row1.size(), row2.size());
        if (ret != 0)
            return ret;

        for (int i = 0; i < row1.size(); i++)
        {
            ret = row1.get(i).compareTo(row2.get(i));
            if (ret != 0)
                return ret;
        }

        return 0;
    };

    protected void assertRowsNet(ProtocolVersion protocolVersion, ResultSet result, Object[]... rows)
    {
        assertRowsNet(protocolVersion, false, result, rows);
    }

    protected void assertRowsNet(ProtocolVersion protocolVersion, boolean ignoreOrder, ResultSet result, Object[] ... rows)
    {
        // necessary as we need cluster objects to supply CodecRegistry.
        // It's reasonably certain that the network setup has already been done
        // by the time we arrive at this point, but adding this check doesn't hurt
        requireNetwork();

        if (result == null)
        {
            if (rows.length > 0)
                fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        ColumnDefinitions meta = result.getColumnDefinitions();

        List<List<ByteBuffer>> expectedRows = new ArrayList<>(rows.length);
        List<List<ByteBuffer>> actualRows = new ArrayList<>(rows.length);

        Cluster cluster = getCluster(protocolVersion);

        com.datastax.driver.core.ProtocolVersion driverVersion = cluster.getConfiguration()
                .getProtocolOptions()
                .getProtocolVersion();

        CodecRegistry codecRegistry = cluster.getConfiguration()
                .getCodecRegistry();

        Iterator<Row> iter = result.iterator();
        int i;
        for (i = 0; i < rows.length; i++)
        {
            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d (using protocol version %s)",
                            i, protocolVersion),
                    meta.size(), rows[i].length);

            assertTrue(String.format("Got fewer rows than expected. Expected %d but got %d", rows.length, i), iter.hasNext());
            Row actual = iter.next();

            List<ByteBuffer> expectedRow = new ArrayList<>(meta.size());
            List<ByteBuffer> actualRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);
                expectedRow.add(codec.serialize(rows[i][j], driverVersion));
                actualRow.add(actual.getBytesUnsafe(meta.getName(j)));
            }

            expectedRows.add(expectedRow);
            actualRows.add(actualRow);
        }

        if (iter.hasNext())
        {
            List<Row> unexpectedRows = new ArrayList<>(2);
            while (iter.hasNext())
            {
                unexpectedRows.add(iter.next());
                i++;
            }

            String[][] formattedRows = new String[actualRows.size() + unexpectedRows.size()][meta.size()];
            for (int k = 0; k < actualRows.size(); k++)
            {
                List<ByteBuffer> row = actualRows.get(k);
                for (int j = 0; j < meta.size(); j++)
                {
                    DataType type = meta.getType(j);
                    com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);

                    formattedRows[k][j] = codec.format(codec.deserialize(row.get(j), driverVersion));
                }
            }

            for (int k = actualRows.size(); k < actualRows.size() + unexpectedRows.size(); k++)
            {
                Row row = unexpectedRows.get(k - actualRows.size());
                for (int j = 0; j < meta.size(); j++)
                {
                    DataType type = meta.getType(j);
                    com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);

                    formattedRows[k][j] = codec.format(codec.deserialize(row.getBytesUnsafe(meta.getName(j)), driverVersion));
                }
            }

            fail(String.format("Got more rows than expected. Expected %d but got %d (using protocol version %s)." +
                            "\nReceived rows:\n%s",
                    rows.length, i, protocolVersion,
                    Arrays.stream(formattedRows).map(Arrays::toString).collect(Collectors.toList())));
        }

        if (ignoreOrder)
        {
            Collections.sort(expectedRows, RowComparator);
            Collections.sort(actualRows, RowComparator);
        }

        for(i = 0; i < expectedRows.size(); i++)
        {
            List<ByteBuffer> expected = expectedRows.get(i);
            List<ByteBuffer> actual = actualRows.get(i);

            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);

                if (!Objects.equal(expected.get(j), actual.get(j)))
                    fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                    "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                    "(using protocol version %s)",
                            i, j, meta.getName(j), type,
                            codec.format(codec.deserialize(expected.get(j),driverVersion)),
                            expected.get(j) == null ? 0 : expected.get(j).capacity(),
                            codec.format(codec.deserialize(actual.get(j), driverVersion)),
                            actual.get(j) == null ? 0 : actual.get(j).capacity(),
                            protocolVersion));
            }
        }
    }

    public Object[][] getRowsNet(Cluster cluster, ColumnDefinitions meta, List<Row> rows)
    {
        if (rows == null || rows.isEmpty())
            return new Object[0][0];

        com.datastax.driver.core.TypeCodec<?>[] codecs = new com.datastax.driver.core.TypeCodec<?>[meta.size()];
        for (int j = 0; j < meta.size(); j++)
            codecs[j] = cluster.getConfiguration().getCodecRegistry().codecFor(meta.getType(j));

        Object[][] ret = new Object[rows.size()][];
        for (int i = 0; i < ret.length; i++)
        {
            Row row = rows.get(i);
            Assert.assertNotNull(row);

            ret[i] = new Object[meta.size()];
            for (int j = 0; j < meta.size(); j++)
                ret[i][j] = row.get(j, codecs[j]);
        }

        return ret;
    }

    public static void assertRows(UntypedResultSet result, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("ROWS:\n{}", String.join("\n", Arrays.stream(getRows(result)).map(row -> Arrays.toString(row)).collect(Collectors.toList())));

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            if (rows[i] == null)
                throw new IllegalArgumentException(String.format("Invalid expected value for row: %d. A row cannot be null.", i));

            Object[] expected = rows[i];
            UntypedResultSet.Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d", i), expected.length, meta.size());

            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);

                ByteBuffer actualValue = actual.getBytes(column.name.toString());
                Object actualValueDecoded = decodeValue(column, actualValue);
                if (!Objects.equal(expected[j], actualValueDecoded))
                {
                    ByteBuffer expectedByteValue = makeByteBuffer(expected[j], column.type);
                    if (!Objects.equal(expectedByteValue, actualValue))
                    {
                        fail(String.format("Invalid value for row %d column %d (%s of type %s), expected <%s> but got <%s>",
                                i,
                                j,
                                column.name,
                                column.type.asCQL3Type(),
                                formatValue(expectedByteValue, column.type),
                                formatValue(actualValue, column.type)));
                    }
                }
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                UntypedResultSet.Row actual = iter.next();
                i++;

                StringBuilder str = new StringBuilder();
                for (int j = 0; j < meta.size(); j++)
                {
                    ColumnSpecification column = meta.get(j);
                    ByteBuffer actualValue = actual.getBytes(column.name.toString());
                    str.append(String.format("%s=%s ", column.name, formatValue(actualValue, column.type)));
                }
                logger.info("Extra row num {}: {}", i, str.toString());
            }
            fail(String.format("Got more rows than expected. Expected %d but got %d.", rows.length, i));
        }

        assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", rows.length>i ? "less" : "more", rows.length, i), i == rows.length);
    }

    private static Object decodeValue(ColumnSpecification column, ByteBuffer actualValue)
    {
        try
        {
            Object actualValueDecoded = actualValue == null ? null : column.type.getSerializer().deserialize(actualValue);
            return actualValueDecoded;
        }
        catch (MarshalException e)
        {
            throw new AssertionError("Cannot deserialize the value for column " + column.name, e);
        }
    }

    /**
     * Like assertRows(), but ignores the ordering of rows.
     */
    public static void assertRowsIgnoringOrder(UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrderInternal(result, false, rows);
    }

    public static void assertRowsIgnoringOrderAndExtra(UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrderInternal(result, true, rows);
    }

    private static void assertRowsIgnoringOrderInternal(UntypedResultSet result, boolean ignoreExtra, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();

        Set<List<ByteBuffer>> expectedRows = new HashSet<>(rows.length);
        for (Object[] expected : rows)
        {
            Assert.assertEquals("Invalid number of (expected) values provided for row", expected.length, meta.size());
            List<ByteBuffer> expectedRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                expectedRow.add(makeByteBuffer(expected[j], meta.get(j).type));
            expectedRows.add(expectedRow);
        }

        Set<List<ByteBuffer>> actualRows = new HashSet<>(result.size());
        for (UntypedResultSet.Row actual : result)
        {
            List<ByteBuffer> actualRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                actualRow.add(actual.getBytes(meta.get(j).name.toString()));
            actualRows.add(actualRow);
        }

        com.google.common.collect.Sets.SetView<List<ByteBuffer>> extra = com.google.common.collect.Sets.difference(actualRows, expectedRows);
        com.google.common.collect.Sets.SetView<List<ByteBuffer>> missing = com.google.common.collect.Sets.difference(expectedRows, actualRows);
        if ((!ignoreExtra && !extra.isEmpty()) || !missing.isEmpty())
        {
            List<String> extraRows = makeRowStrings(extra, meta);
            List<String> missingRows = makeRowStrings(missing, meta);
            StringBuilder sb = new StringBuilder();
            if (!extra.isEmpty())
            {
                sb.append("Got ").append(extra.size()).append(" extra row(s) ");
                if (!missing.isEmpty())
                    sb.append("and ").append(missing.size()).append(" missing row(s) ");
                sb.append("in result.  Extra rows:\n    ");
                sb.append(extraRows.stream().collect(Collectors.joining("\n    ")));
                if (!missing.isEmpty())
                    sb.append("\nMissing Rows:\n    ").append(missingRows.stream().collect(Collectors.joining("\n    ")));
                fail(sb.toString());
            }

            if (!missing.isEmpty())
                fail("Missing " + missing.size() + " row(s) in result: \n    " + missingRows.stream().collect(Collectors.joining("\n    ")));
        }

        assert ignoreExtra || expectedRows.size() == actualRows.size();
    }

    protected static List<String> makeRowStrings(UntypedResultSet resultSet)
    {
        List<List<ByteBuffer>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            List<ByteBuffer> values = new ArrayList<>();
            for (ColumnSpecification columnSpecification : resultSet.metadata())
            {
                values.add(row.getBytes(columnSpecification.name.toString()));
            }
            rows.add(values);
        }

        return makeRowStrings(rows, resultSet.metadata());
    }

    private static List<String> makeRowStrings(Iterable<List<ByteBuffer>> rows, List<ColumnSpecification> meta)
    {
        List<String> strings = new ArrayList<>();
        for (List<ByteBuffer> row : rows)
        {
            StringBuilder sb = new StringBuilder("row(");
            for (int j = 0; j < row.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                sb.append(column.name.toString()).append("=").append(formatValue(row.get(j), column.type));
                if (j < (row.size() - 1))
                    sb.append(", ");
            }
            strings.add(sb.append(")").toString());
        }
        return strings;
    }

    protected void assertRowCount(UntypedResultSet result, int numExpectedRows)
    {
        if (result == null)
        {
            if (numExpectedRows > 0)
                fail(String.format("No rows returned by query but %d expected", numExpectedRows));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < numExpectedRows)
        {
            UntypedResultSet.Row actual = iter.next();
            assertNotNull(actual);
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            fail(String.format("Got less rows than expected. Expected %d but got %d.", numExpectedRows, i));
        }

        assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", numExpectedRows>i ? "less" : "more", numExpectedRows, i), i == numExpectedRows);
    }

    protected static Object[][] getRows(UntypedResultSet result)
    {
        if (result == null)
            return new Object[0][];

        List<Object[]> ret = new ArrayList<>();
        List<ColumnSpecification> meta = result.metadata();

        Iterator<UntypedResultSet.Row> iter = result.iterator();
        while (iter.hasNext())
        {
            UntypedResultSet.Row rowVal = iter.next();
            Object[] row = new Object[meta.size()];
            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer val = rowVal.getBytes(column.name.toString());
                row[j] = val == null ? null : column.type.getSerializer().deserialize(val);
            }

            ret.add(row);
        }

        Object[][] a = new Object[ret.size()][];
        return ret.toArray(a);
    }

    protected void assertColumnNames(UntypedResultSet result, String... expectedColumnNames)
    {
        if (result == null)
        {
            fail("No rows returned by query.");
            return;
        }

        List<ColumnSpecification> metadata = result.metadata();
        Assert.assertEquals("Got less columns than expected.", expectedColumnNames.length, metadata.size());

        for (int i = 0, m = metadata.size(); i < m; i++)
        {
            ColumnSpecification columnSpec = metadata.get(i);
            Assert.assertEquals(expectedColumnNames[i], columnSpec.name.toString());
        }
    }

    protected void assertAllRows(Object[]... rows) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), rows);
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }

    protected void assertEmpty(UntypedResultSet result) throws Throwable
    {
        if (result != null && !result.isEmpty())
            throw new AssertionError(String.format("Expected empty result but got %d rows: %s \n", result.size(), makeRowStrings(result)));
    }

    protected void assertInvalid(String query, Object... values) throws Throwable
    {
        assertInvalidMessage(null, query, values);
    }

    /**
     * Checks that the specified query is not autorized for the current user.
     * @param errorMessage The expected error message
     * @param query the query
     * @param values the query parameters
     */
    protected void assertUnauthorizedQuery(String errorMessage, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                errorMessage,
                UnauthorizedException.class,
                query,
                values);
    }

    protected void assertInvalidMessageNet(String errorMessage, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT), errorMessage, null, query, values);
    }

    protected void assertInvalidCreateTable(String errorMessage, String createTableStatement, Object... values) throws Throwable
    {
        createTableName();
        assertInvalidThrowMessage(errorMessage, null, createTableStatement, values);
    }

    protected void assertInvalidMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(errorMessage, null, query, values);
    }

    protected void assertInvalidThrow(Class<? extends Throwable> exception, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(null, exception, query, values);
    }

    protected void assertInvalidThrowMessage(String errorMessage, Class<? extends Throwable> exception, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.empty(), errorMessage, exception, query, values);
    }

    // if a protocol version > Integer.MIN_VALUE is supplied, executes
    // the query via the java driver, mimicking a real client.
    protected void assertInvalidThrowMessage(Optional<ProtocolVersion> protocolVersion,
                                             String errorMessage,
                                             Class<? extends Throwable> exception,
                                             String query,
                                             Object... values) throws Throwable
    {
        try
        {
            if (!protocolVersion.isPresent())
                execute(query, values);
            else
                executeNet(protocolVersion.get(), query, values);

            fail("Query should be invalid but no error was thrown. Query is: " + queryInfo(query, values));
        }
        catch (Exception e)
        {
            if (exception != null && !exception.isAssignableFrom(e.getClass()))
            {
                fail("Query should be invalid but wrong error was thrown. " +
                        "Expected: " + exception.getName() + ", got: " + e.getClass().getName() + ". " +
                        "message: '" + e.getMessage() + "'. " +
                        "Query is: " + queryInfo(query, values) + ". Stack trace of unexpected exception is:\n" +
                        String.join("\n", Arrays.stream(e.getStackTrace())
                                .map(t -> t.toString())
                                .collect(Collectors.toList())));
            }
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    protected void assertClientWarning(String warningMessage,
                                       String query,
                                       Object... values) throws Throwable
    {
        assertClientWarning(getDefaultVersion(), warningMessage, query, values);
    }

    protected void assertClientWarning(ProtocolVersion protocolVersion,
                                       String warningMessage,
                                       String query,
                                       Object... values) throws Throwable
    {
        assertClientWarning(protocolVersion, Collections.emptyList(), warningMessage, query, values);
    }

    protected void assertClientWarning(List<String> ignoredWarnings,
                                       String warningMessage,
                                       String query,
                                       Object... values) throws Throwable
    {
        assertClientWarning(getDefaultVersion(), ignoredWarnings, warningMessage, query, values);
    }

    protected void assertClientWarning(ProtocolVersion protocolVersion,
                                       List<String> ignoredWarnings,
                                       String warningMessage,
                                       String query,
                                       Object... values) throws Throwable
    {
        ResultSet rs = executeNet(protocolVersion, query, values);
        List<String> warnings = warningsFromResultSet(ignoredWarnings, rs);
        assertNotNull("Expecting one warning but get none", warnings);
        assertTrue("Expecting one warning but get " + warnings.size(), warnings.size() == 1);
        assertTrue("Expecting warning message to contains " + warningMessage + " but was: " + warnings.get(0),
                warnings.get(0).contains(warningMessage));
    }

    protected void assertNoClientWarning(String query,
                                         Object... values) throws Throwable
    {
        assertNoClientWarning(getDefaultVersion(), query, values);
    }

    protected void assertNoClientWarning(ProtocolVersion protocolVersion,
                                         String query,
                                         Object... values) throws Throwable
    {
        assertNoClientWarning(protocolVersion, Collections.emptyList(), query, values);
    }

    protected void assertNoClientWarning(List<String> ignoredWarnings,
                                         String query,
                                         Object... values) throws Throwable
    {
        assertNoClientWarning(getDefaultVersion(), ignoredWarnings, query, values);
    }

    protected void assertNoClientWarning(ProtocolVersion protocolVersion,
                                         List<String> ignoredWarnings,
                                         String query,
                                         Object... values) throws Throwable
    {
        ResultSet rs = executeNet(protocolVersion, query, values);
        List<String> warnings = warningsFromResultSet(ignoredWarnings, rs);
        assertTrue("Expecting no warning but get some: " + warnings, warnings.isEmpty());
    }

    private static List<String> warningsFromResultSet(List<String> ignoredWarnings, ResultSet rs)
    {
        return rs.getExecutionInfo().getWarnings()
                .stream().filter(w -> ignoredWarnings.stream().noneMatch(w::contains))
                .collect(Collectors.toList());
    }

    private static String queryInfo(String query, Object[] values)
    {
        return USE_PREPARED_VALUES
                ? query + " (values: " + formatAllValues(values) + ")"
                : replaceValues(query, values);
    }

    protected CQLStatement.Raw assertValidSyntax(String query) throws Throwable
    {
        try
        {
            return QueryProcessor.parseStatement(query);
        }
        catch(SyntaxException e)
        {
            fail(String.format("Expected query syntax to be valid but was invalid. Query is: %s; Error is %s",
                    query, e.getMessage()));
            return null;
        }
    }

    protected void assertInvalidSyntax(String query, Object... values) throws Throwable
    {
        assertInvalidSyntaxMessage(null, query, values);
    }

    protected void assertInvalidSyntaxMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            fail("Query should have invalid syntax but no error was thrown. Query is: " + queryInfo(query, values));
        }
        catch (SyntaxException e)
        {
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    /**
     * Asserts that the message of the specified exception contains the specified text.
     *
     * @param text the text that the exception message must contains
     * @param e the exception to check
     */
    private static void assertMessageContains(String text, Exception e)
    {
        assertNotNull("Expected error to have message '" + text + "' but error has no message at all (error "
                + "was of type " + e.getClass().getSimpleName() + ")", e.getMessage());
        assertTrue("Expected error message to contain '" + text + "', but got '" + e.getMessage() + "' (error "
                        + "was of type " + e.getClass().getSimpleName() + ")",
                e.getMessage().contains(text));
    }

    /**
     * Sorts a list of int32 keys by their Murmur3Partitioner token order.
     */
    public static List<Integer> partitionerSortedKeys(List<Integer> unsortedKeys)
    {
        List<DecoratedKey> decoratedKeys = unsortedKeys.stream().map(i -> Murmur3Partitioner.instance.decorateKey(Int32Type.instance.getSerializer().serialize(i))).collect(Collectors.toList());
        Collections.sort(decoratedKeys, DecoratedKey.comparator);
        return decoratedKeys.stream().map(dk -> Int32Type.instance.getSerializer().deserialize(dk.getKey())).collect(Collectors.toList());
    }

    @FunctionalInterface
    public interface CheckedFunction {
        void apply() throws Throwable;
    }

    /**
     * Runs the given function before and after a flush of sstables.  This is useful for checking that behavior is
     * the same whether data is in memtables or sstables.
     * @param runnable
     * @throws Throwable
     */
    public void beforeAndAfterFlush(CheckedFunction runnable) throws Throwable
    {
        runnable.apply();
        flush();
        runnable.apply();
    }

    private static String replaceValues(String query, Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        int last = 0;
        int i = 0;
        int idx;
        while ((idx = query.indexOf('?', last)) > 0)
        {
            if (i >= values.length)
                throw new IllegalArgumentException(String.format("Not enough values provided. The query has at least %d variables but only %d values provided", i, values.length));

            sb.append(query.substring(last, idx));

            Object value = values[i++];

            // When we have a .. IN ? .., we use a list for the value because that's what's expected when the value is serialized.
            // When we format as string however, we need to special case to use parenthesis. Hackish but convenient.
            if (idx >= 3 && value instanceof List && query.substring(idx - 3, idx).equalsIgnoreCase("IN "))
            {
                List l = (List)value;
                sb.append("(");
                for (int j = 0; j < l.size(); j++)
                {
                    if (j > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(j)));
                }
                sb.append(")");
            }
            else
            {
                sb.append(formatForCQL(value));
            }
            last = idx + 1;
        }
        sb.append(query.substring(last));
        return sb.toString();
    }

    // We're rellly only returning ByteBuffers but this make the type system happy
    protected static Object[] transformValues(Object[] values)
    {
        // We could partly rely on QueryProcessor.executeOnceInternal doing type conversion for us, but
        // it would complain with ClassCastException if we pass say a string where an int is excepted (since
        // it bases conversion on what the value should be, not what it is). For testing, we sometimes
        // want to pass value of the wrong type and assert that this properly raise an InvalidRequestException
        // and executeOnceInternal goes into way. So instead, we pre-convert everything to bytes here based
        // on the value.
        // Besides, we need to handle things like TupleValue that executeOnceInternal don't know about.

        Object[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                buffers[i] = null;
                continue;
            }
            else if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                buffers[i] = ByteBufferUtil.UNSET_BYTE_BUFFER;
                continue;
            }

            try
            {
                buffers[i] = typeFor(value).decompose(serializeTuples(value));
            }
            catch (Exception ex)
            {
                logger.info("Error serializing query parameter {}:", value, ex);
                throw ex;
            }
        }
        return buffers;
    }

    private static Object serializeTuples(Object value)
    {
        if (value instanceof TupleValue)
        {
            return ((TupleValue)value).toByteBuffer();
        }

        // We need to reach inside collections for TupleValue and transform them to ByteBuffer
        // since otherwise the decompose method of the collection AbstractType won't know what
        // to do with them
        if (value instanceof List)
        {
            List l = (List)value;
            List n = new ArrayList(l.size());
            for (Object o : l)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            Set n = new LinkedHashSet(s.size());
            for (Object o : s)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            Map n = new LinkedHashMap(m.size());
            for (Object entry : m.entrySet())
                n.put(serializeTuples(((Map.Entry)entry).getKey()), serializeTuples(((Map.Entry)entry).getValue()));
            return n;
        }
        return value;
    }

    private static String formatAllValues(Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(formatForCQL(values[i]));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatForCQL(Object value)
    {
        if (value == null)
            return "null";

        if (value instanceof TupleValue)
            return ((TupleValue)value).toCQLString();

        // We need to reach inside collections for TupleValue. Besides, for some reason the format
        // of collection that CollectionType.getString gives us is not at all 'CQL compatible'
        if (value instanceof Collection || value instanceof Map)
        {
            StringBuilder sb = new StringBuilder();
            if (value instanceof List)
            {
                List l = (List)value;
                sb.append("[");
                for (int i = 0; i < l.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(i)));
                }
                sb.append("]");
            }
            else if (value instanceof Set)
            {
                Set s = (Set)value;
                sb.append("{");
                Iterator iter = s.iterator();
                while (iter.hasNext())
                {
                    sb.append(formatForCQL(iter.next()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            else
            {
                Map m = (Map)value;
                sb.append("{");
                Iterator iter = m.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry)iter.next();
                    sb.append(formatForCQL(entry.getKey())).append(": ").append(formatForCQL(entry.getValue()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            return sb.toString();
        }

        AbstractType type = typeFor(value);
        String s = type.getString(type.decompose(value));

        if (type instanceof InetAddressType || type instanceof TimestampType)
            return String.format("'%s'", s);
        else if (type instanceof UTF8Type)
            return String.format("'%s'", s.replaceAll("'", "''"));
        else if (type instanceof BytesType)
            return "0x" + s;

        return s;
    }

    protected static ByteBuffer makeByteBuffer(Object value, AbstractType type)
    {
        if (value == null)
            return null;

        if (value instanceof TupleValue)
            return ((TupleValue)value).toByteBuffer();

        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        return type.decompose(serializeTuples(value));
    }

    protected static String formatValue(ByteBuffer bb, AbstractType<?> type)
    {
        if (bb == null)
            return "null";

        if (type instanceof CollectionType)
        {
            // CollectionType override getString() to use hexToBytes. We can't change that
            // without breaking SSTable2json, but the serializer for collection have the
            // right getString so using it directly instead.
            TypeSerializer ser = type.getSerializer();
            return ser.toString(ser.deserialize(bb));
        }

        return type.getString(bb);
    }

    protected TupleValue tuple(Object...values)
    {
        return new TupleValue(values);
    }

    protected Object userType(Object... values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("userType() requires an even number of arguments");

        String[] fieldNames = new String[values.length / 2];
        Object[] fieldValues = new Object[values.length / 2];
        int fieldNum = 0;
        for (int i = 0; i < values.length; i += 2)
        {
            fieldNames[fieldNum] = (String) values[i];
            fieldValues[fieldNum] = values[i + 1];
            fieldNum++;
        }
        return new UserTypeValue(fieldNames, fieldValues);
    }

    protected static Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    protected static Set set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    protected static Map map(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("Invalid number of arguments, got " + values.length);

        int size = values.length / 2;
        Map m = new LinkedHashMap(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    protected com.datastax.driver.core.TupleType tupleTypeOf(ProtocolVersion protocolVersion, DataType...types)
    {
        requireNetwork();
        return getCluster(protocolVersion).getMetadata().newTupleType(types);
    }

    // Attempt to find an AbstracType from a value (for serialization/printing sake).
    // Will work as long as we use types we know of, which is good enough for testing
    private static AbstractType typeFor(Object value)
    {
        if (value instanceof ByteBuffer || value instanceof TupleValue || value == null)
            return BytesType.instance;

        if (value instanceof Byte)
            return ByteType.instance;

        if (value instanceof Short)
            return ShortType.instance;

        if (value instanceof Integer)
            return Int32Type.instance;

        if (value instanceof Long)
            return LongType.instance;

        if (value instanceof Float)
            return FloatType.instance;

        if (value instanceof Duration)
            return DurationType.instance;

        if (value instanceof Double)
            return DoubleType.instance;

        if (value instanceof BigInteger)
            return IntegerType.instance;

        if (value instanceof BigDecimal)
            return DecimalType.instance;

        if (value instanceof String)
            return UTF8Type.instance;

        if (value instanceof Boolean)
            return BooleanType.instance;

        if (value instanceof InetAddress)
            return InetAddressType.instance;

        if (value instanceof Date)
            return TimestampType.instance;

        if (value instanceof UUID)
            return UUIDType.instance;

        if (value instanceof List)
        {
            List l = (List)value;
            AbstractType elt = l.isEmpty() ? BytesType.instance : typeFor(l.get(0));
            return ListType.getInstance(elt, true);
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            AbstractType elt = s.isEmpty() ? BytesType.instance : typeFor(s.iterator().next());
            return SetType.getInstance(elt, true);
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            AbstractType keys, values;
            if (m.isEmpty())
            {
                keys = BytesType.instance;
                values = BytesType.instance;
            }
            else
            {
                Map.Entry entry = (Map.Entry)m.entrySet().iterator().next();
                keys = typeFor(entry.getKey());
                values = typeFor(entry.getValue());
            }
            return MapType.getInstance(keys, values, true);
        }

        throw new IllegalArgumentException("Unsupported value type (value is " + value + ")");
    }

    private static class TupleValue
    {
        protected final Object[] values;

        TupleValue(Object[] values)
        {
            this.values = values;
        }

        public ByteBuffer toByteBuffer()
        {
            ByteBuffer[] bbs = new ByteBuffer[values.length];
            for (int i = 0; i < values.length; i++)
                bbs[i] = makeByteBuffer(values[i], typeFor(values[i]));
            return TupleType.buildValue(bbs);
        }

        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < values.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(formatForCQL(values[i]));
            }
            sb.append(")");
            return sb.toString();
        }

        public String toString()
        {
            return "TupleValue" + toCQLString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TupleValue that = (TupleValue) o;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(values);
        }
    }

    private static class UserTypeValue extends TupleValue
    {
        private final String[] fieldNames;

        UserTypeValue(String[] fieldNames, Object[] fieldValues)
        {
            super(fieldValues);
            this.fieldNames = fieldNames;
        }

        @Override
        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean haveEntry = false;
            for (int i = 0; i < values.length; i++)
            {
                if (values[i] != null)
                {
                    if (haveEntry)
                        sb.append(", ");
                    sb.append(ColumnIdentifier.maybeQuote(fieldNames[i]));
                    sb.append(": ");
                    sb.append(formatForCQL(values[i]));
                    haveEntry = true;
                }
            }
            assert haveEntry;
            sb.append("}");
            return sb.toString();
        }

        public String toString()
        {
            return "UserTypeValue" + toCQLString();
        }
    }

    private static class User
    {
        /**
         * The user name
         */
        public final String username;

        /**
         * The user password
         */
        public final String password;

        public User(String username, String password)
        {
            this.username = username;
            this.password = password;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(username, password);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof User))
                return false;

            User u = (User) o;

            return Objects.equal(username, u.username)
                    && Objects.equal(password, u.password);
        }
    }

    public static class Randomization
    {
        private long seed;
        private Random random;

        Randomization()
        {
            if (random == null)
            {
                seed = PropertyConfiguration.getLong("cassandra.test.random.seed", System.nanoTime());
                random = new Random(seed);
            }
        }

        public void printSeedOnFailure()
        {
            System.err.println("Randomized test failed. To rerun test use -PrandomSeed=" + seed);
        }

        public int nextInt()
        {
            return random.nextInt();
        }

        public int nextIntBetween(int minValue, int maxValue)
        {
            return RandomInts.randomIntBetween(random, minValue, maxValue);
        }

        public long nextLong()
        {
            return random.nextLong();
        }

        public short nextShort()
        {
            return (short)random.nextInt(Short.MAX_VALUE + 1);
        }

        public byte nextByte()
        {
            return (byte)random.nextInt(Byte.MAX_VALUE + 1);
        }

        public BigInteger nextBigInteger(int minNumBits, int maxNumBits)
        {
            return new BigInteger(RandomInts.randomIntBetween(random, minNumBits, maxNumBits), random);
        }

        public BigDecimal nextBigDecimal(int minUnscaledValue, int maxUnscaledValue, int minScale, int maxScale)
        {
            return BigDecimal.valueOf(RandomInts.randomIntBetween(random, minUnscaledValue, maxUnscaledValue),
                    RandomInts.randomIntBetween(random, minScale, maxScale));
        }

        public float nextFloat()
        {
            return random.nextFloat();
        }

        public double nextDouble()
        {
            return random.nextDouble();
        }

        public String nextAsciiString(int minLength, int maxLength)
        {
            return RandomStrings.randomAsciiOfLengthBetween(random, minLength, maxLength);
        }

        public String nextTextString(int minLength, int maxLength)
        {
            return RandomStrings.randomRealisticUnicodeOfLengthBetween(random, minLength, maxLength);
        }

        public boolean nextBoolean()
        {
            return random.nextBoolean();
        }

        public void nextBytes(byte[] bytes)
        {
            random.nextBytes(bytes);
        }
    }

    public static class FailureWatcher extends TestWatcher
    {
        @Override
        protected void failed(Throwable e, Description description)
        {
            if (random != null)
                random.printSeedOnFailure();
        }
    }
}