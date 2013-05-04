package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records.RequestRecord;

public enum IPingRequest implements RequestRecord, Operation.XidHeader {
    PING_REQUEST;
    
    public static IPingRequest getInstance() {
        return PING_REQUEST;
    }
    
    public static final OpCodeXid OPCODE_XID = OpCodeXid.PING;
    public static final OpCode OPCODE = OPCODE_XID.opcode();
    public static final int XID = OPCODE_XID.xid();
    
    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public int xid() {
        return XID;
    }

    @Override
    public void serialize(OutputArchive archive, String tag) {}

    @Override
    public void serialize(OutputArchive archive) {}
    
    @Override
    public void deserialize(InputArchive archive, String tag) {}
    
    @Override
    public void deserialize(InputArchive archive) {}
}