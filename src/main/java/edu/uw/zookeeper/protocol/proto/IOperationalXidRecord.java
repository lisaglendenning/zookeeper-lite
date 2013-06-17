package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;

import edu.uw.zookeeper.protocol.Operation;

public abstract class IOperationalXidRecord<T extends Record> extends IOperationalRecord<T> implements Operation.Action, Operation.XidHeader {

    protected IOperationalXidRecord(T record) {
        super(record);
    }

    @Override
    public int xid() {
        return Records.opCodeXidOf(getClass()).xid();
    }
}
