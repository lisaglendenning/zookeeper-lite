package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import edu.uw.zookeeper.protocol.OpCode;

public enum IDeleteResponse implements Records.ResponseRecord, Records.DataRecord {
    DELETE_RESPONSE;
    
    public static IDeleteResponse getInstance() {
        return DELETE_RESPONSE;
    }
    
    public static final OpCode OPCODE = OpCode.DELETE;
    
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