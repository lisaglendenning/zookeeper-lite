package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;

public abstract class ICodedRecord<T extends Record> extends IRecord<T> implements Records.Coded {

    protected ICodedRecord(T record) {
        super(record);
    }

    @Override
    public OpCode getOpcode() {
        return Records.opCodeOf(getClass());
    }
}
