package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;

public abstract class IOperationalRecord<T extends Record> extends IRecord<T> implements Records.Coded {

    protected IOperationalRecord(T record) {
        super(record);
    }

    @Override
    public OpCode opcode() {
        return Records.opCodeOf(getClass());
    }
}
