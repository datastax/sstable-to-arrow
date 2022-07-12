package com.datastax.cndb.sstable;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import de.huxhorn.sulky.ulid.ULID;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableUniqueIdentifierFactory;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SSTable generation identifiers that can be stored across nodes in one directory/bucket
 *
 * Uses the ULID identifiers which are lexicographically sortable by time: https://github.com/ulid/spec
 *
 * For backwards compatibility we can also read sequence based for upgrade/import purposes
 */
public class ULIDBasedSSTableUniqueIdentifierFactory extends SSTableUniqueIdentifier.Factory
{
    private static final ULID ulid = new ULID();
    private static final int ULID_LEN_STRING = 26;

    private SequenceBasedSSTableUniqueIdentifierFactory sequenceDelegate = new SequenceBasedSSTableUniqueIdentifierFactory(Optional.empty());

    public ULIDBasedSSTableUniqueIdentifierFactory(Optional<Directories.SSTableLister> ssTableLister)
    {
        super(ssTableLister);
        //Not used
    }

    @Override
    public SSTableUniqueIdentifier create()
    {
        return new ULIDBasedSSTableUniqueIdentifier(ulid.nextValue());
    }

    @Override
    public SSTableUniqueIdentifier fromString(String s)
    {
        Preconditions.checkArgument(s != null, "Identifier must not be null");

        if (s.length() == ULID_LEN_STRING)
        {
            return new ULIDBasedSSTableUniqueIdentifier(ULID.parseULID(s));
        }

        return sequenceDelegate.fromString(s);
    }

    @Override
    public SSTableUniqueIdentifier fromBytes(ByteBuffer byteBuffer)
    {
        Preconditions.checkArgument(byteBuffer != null, "Identifier must not be null");

        if (byteBuffer.remaining() == Ints.BYTES)
            return sequenceDelegate.fromBytes(byteBuffer);

        return new ULIDBasedSSTableUniqueIdentifier(ULID.fromBytes(ByteBufferUtil.getArray(byteBuffer)));
    }

    public static class ULIDBasedSSTableUniqueIdentifier extends SSTableUniqueIdentifier
    {
        private final ULID.Value ulid;

        public ULIDBasedSSTableUniqueIdentifier(ULID.Value ulid)
        {
            this.ulid = ulid;
        }

        @Override
        public ByteBuffer toBytes()
        {
            return ByteBuffer.wrap(ulid.toBytes());
        }

        @Override
        public String toString()
        {
            return ulid.toString();
        }

        @Override
        public int compareTo(SSTableUniqueIdentifier o)
        {
            //Assumed sstables with a diff identifier are old
            return o instanceof ULIDBasedSSTableUniqueIdentifier ? ulid.compareTo(((ULIDBasedSSTableUniqueIdentifier) o).ulid) : +1;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ULIDBasedSSTableUniqueIdentifier that = (ULIDBasedSSTableUniqueIdentifier) o;
            return ulid.equals(that.ulid);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(ulid);
        }
    }

}
