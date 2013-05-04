package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import edu.uw.zookeeper.protocol.OpCode;

public enum IDisconnectResponse implements Records.ResponseRecord {
    DISCONNECT_RESPONSE;
    
    public static IDisconnectResponse getInstance() {
        return DISCONNECT_RESPONSE;
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