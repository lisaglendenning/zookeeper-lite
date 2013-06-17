package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;

import edu.uw.zookeeper.protocol.Operation;

public abstract class IOperationalRecord<T extends Record> extends IRecord<T> implements Operation.Action {

    protected IOperationalRecord(T record) {
        super(record);
    }

    @Override
    public OpCode opcode() {
        return Records.opCodeOf(getClass());
    }
}
