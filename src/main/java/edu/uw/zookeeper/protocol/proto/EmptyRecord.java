package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Singleton;

public final class EmptyRecord implements Record {

    public static EmptyRecord getInstance() {
        return Holder.INSTANCE.get();
    }
    
    public static enum Holder implements Singleton<EmptyRecord> {
        INSTANCE(new EmptyRecord());
        
        private final EmptyRecord instance;
        
        private Holder(EmptyRecord instance) {
            this.instance = instance;
        }
        
        @Override
        public EmptyRecord get() {
            return instance;
        }
    }
    
    private EmptyRecord() {}
    
    @Override
    public void serialize(OutputArchive archive, String tag) {
    }

    @Override
    public void deserialize(InputArchive archive, String tag) {
    }
    
    @Override
    public String toString() {
        return "";
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof EmptyRecord)) {
            return false;
        }
        return true;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getClass());
    }
}
