package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.RequestRecord;

public enum IDisconnectRequest implements RequestRecord {
    INSTANCE;
    
    public static IDisconnectRequest getInstance() {
        return INSTANCE;
    }
    
    public static final OpCode OPCODE = OpCode.CLOSE_SESSION;
    
    @Override
    public OpCode opcode() {
        return OPCODE;
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