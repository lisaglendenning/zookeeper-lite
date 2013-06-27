package edu.uw.zookeeper.protocol.proto;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Reference;

public class IRecord<T extends Record> implements Reference<T>, Record {

    public static <T extends Record> IRecord<T> valueOf(T record) {
        return new IRecord<T>(record);
    }
    
    protected final T record;

    protected IRecord(T record) {
        this.record = checkNotNull(record);
    }

    @Override
    public T get() {
        return record;
    }

    @Override
    public String toString() {
        return Records.toBeanString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(get());
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
        return Objects.equal(get(), other.get());
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        get().serialize(archive, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag)
            throws IOException {
        get().deserialize(archive, tag);
    }
}
