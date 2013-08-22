package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.RequestHeader;


public class IRequestHeader extends IRecord<RequestHeader> implements Records.Header {

    public IRequestHeader() {
        this(new RequestHeader());
    }

    public IRequestHeader(int xid, OpCode opcode) {
        this(xid, opcode.intValue());
    }

    public IRequestHeader(int xid, int type) {
        this(new RequestHeader(xid, type));
    }

    public IRequestHeader(RequestHeader record) {
        super(record);
    }

    public int getType() {
        return record.getType();
    }
    
    public int getXid() {
        return record.getXid();
    }
}
