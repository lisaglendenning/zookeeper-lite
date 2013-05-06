package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import edu.uw.zookeeper.protocol.OpCode;

public enum ICheckVersionResponse implements Records.ResponseRecord, Records.MultiOpResponse {
    CHECK_VERSION_RESPONSE;
    
    public static ICheckVersionResponse getInstance() {
        return CHECK_VERSION_RESPONSE;
    }
    
    public static final OpCode OPCODE = OpCode.CHECK;
    
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