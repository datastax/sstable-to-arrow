package com.datastax.cndb.bulkimport;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;

public class BulkImporterApplication extends Application
{

    private Set<Object> singletons = new HashSet<>();

    public BulkImporterApplication() {
        super();
        singletons.add(new BulkImporterHttpResource());
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

    public static void init()
    {
        // disable debug logs for this class
        Logger logger = (Logger) LoggerFactory.getLogger("org.apache.http.wire");
        logger.setLevel(Level.ERROR);
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.loadSchema();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }
}
