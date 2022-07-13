package com.datastax.sstablearrow;

import com.datastax.cndb.sstable.ULIDBasedSSTableUniqueIdentifierFactory;
import de.huxhorn.sulky.ulid.ULID;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Shared utilities for initializing a local instance of Cassandra and getting
 * SSTable descriptors.
 */
public class DescriptorUtils
{

    /**
     * Gets a directory nested two layers deep under basePath with the given keyspace and table name.
     * @param basePath the path to extend
     * @param metadata the table to get the directory for
     * @return the path to the new nested directory
     */
    public static Path addKeyspaceAndTable(Path basePath, TableMetadata metadata) {
        return basePath.resolve(metadata.keyspace).resolve(metadata.name);
    }

    /**
     * Create a unique Descriptor from the path to a local Data.db file
     * and a generated ULID.
     * @param localDataFile the path to a local Data.db file
     * @param ulid the ULID to use for the descriptor
     * @return the Descriptor for the SSTable
     */
    public static Descriptor descriptorWithUlidGeneration(Path localDataFile, ULIDBasedSSTableUniqueIdentifierFactory.ULIDBasedSSTableUniqueIdentifier ulid) {
        Descriptor d = Descriptor.fromFilename(localDataFile);
        return new Descriptor(d.getDirectory(), d.ksname, d.cfname, ulid, d.formatType);
    }

    public static Descriptor descriptorWithUlidGeneration(Path localDataFile) {
        return descriptorWithUlidGeneration(localDataFile, fromNextValue());
    }

    public static ULIDBasedSSTableUniqueIdentifierFactory.ULIDBasedSSTableUniqueIdentifier fromNextValue() {
        return new ULIDBasedSSTableUniqueIdentifierFactory.ULIDBasedSSTableUniqueIdentifier(new ULID().nextValue());
    }

}
