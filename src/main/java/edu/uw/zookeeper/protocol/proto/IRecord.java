package edu.uw.zookeeper.protocol.proto;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import com.google.common.base.Objects;

public class IRecord<T extends Record> implements Record {

    public static <T extends Record> IRecord<T> valueOf(T record) {
        return new IRecord<T>(record);
    }
    
    protected final T record;

    protected IRecord(T record) {
        this.record = checkNotNull(record);
    }

    @Override
    public String toString() {
        return Records.toBeanString(this);
    }

    @Override
    public int hashCode() {
        return record.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IRecord<?> other = (IRecord<?>) obj;
        return Objects.equal(record, other.record);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        record.serialize(archive, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag)
            throws IOException {
        record.deserialize(archive, tag);
    }
}
