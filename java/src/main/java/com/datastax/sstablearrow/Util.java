package com.datastax.sstablearrow;

import de.huxhorn.sulky.ulid.ULID;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;

public class Util {

    public static boolean initialized = false;

    public static void init() {
        if (!initialized) {
            System.setProperty("cassandra.system_view.only_local_and_peers_table", "true");
            System.setProperty("cassandra.config", "file:///" + System.getProperty("user.dir") + "/src/resources/cassandra.yaml");

            DatabaseDescriptor.daemonInitialization();

            try {
                SchemaLoader.cleanupAndLeaveDirs();
            } catch (IOException e) {
                System.err.println("Error initializing SchemaLoader: " + e.getMessage());
                System.exit(1);
            }

            Keyspace.setInitialized();
            StorageService.instance.initServer();

            initialized = true;
        }
    }

    public static Path addKeyspaceAndTable(Path p, TableMetadata metadata) {
        return p.resolve(metadata.keyspace + File.separator + metadata.name);
    }


    public static Descriptor descriptorWithUlidGeneration(Path localDataFile, ULIDBasedSSTableUniqueIdentifier ulid) {
        Descriptor d = Descriptor.fromFilename(localDataFile);
        Descriptor newDescriptor = new Descriptor(d.getDirectory(), d.ksname, d.cfname, ulid, d.formatType);
        return newDescriptor;
    }

    public static Descriptor descriptorWithUlidGeneration(Path localDataFile) {
        return descriptorWithUlidGeneration(localDataFile, ULIDBasedSSTableUniqueIdentifier.fromNextValue());
    }

    public static class ULIDBasedSSTableUniqueIdentifier extends SSTableUniqueIdentifier {
        public final ULID.Value ulid;

        public ULIDBasedSSTableUniqueIdentifier(ULID.Value ulid) {
            this.ulid = ulid;
        }

        public static ULIDBasedSSTableUniqueIdentifier fromNextValue() {
            return new Util.ULIDBasedSSTableUniqueIdentifier(new ULID().nextValue());
        }

        @Override
        public ByteBuffer toBytes() {
            return ByteBuffer.wrap(ulid.toBytes());
        }

        @Override
        public String toString() {
            return ulid.toString();
        }

        @Override
        public int compareTo(SSTableUniqueIdentifier o) {
            //Assumed sstables with a diff identifier are old
            return o instanceof ULIDBasedSSTableUniqueIdentifier ? ulid.compareTo(((ULIDBasedSSTableUniqueIdentifier) o).ulid) : +1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ULIDBasedSSTableUniqueIdentifier that = (ULIDBasedSSTableUniqueIdentifier) o;
            return ulid.equals(that.ulid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ulid);
        }
    }


}
