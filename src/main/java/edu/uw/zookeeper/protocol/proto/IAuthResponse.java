package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;

public enum IAuthResponse implements ResponseRecord, Operation.XidHeader {
    AUTH_RESPONSE;
    
    public static IAuthResponse getInstance() {
        return AUTH_RESPONSE;
    }
    
    public static final OpCodeXid OPCODE_XID = OpCodeXid.AUTH;
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