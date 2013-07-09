package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;

import edu.uw.zookeeper.protocol.Operation;

public abstract class IOpCodeXidRecord<T extends Record> extends ICodedRecord<T> implements Operation.RequestId {

    protected IOpCodeXidRecord(T record) {
        super(record);
    }

    @Override
    public int xid() {
        return Records.opCodeXidOf(getClass()).xid();
    }
}
