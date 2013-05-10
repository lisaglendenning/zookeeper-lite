package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.SyncResponse;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;
import edu.uw.zookeeper.protocol.proto.Records.Responses;

public class ISyncResponse extends SyncResponse implements ResponseRecord, Records.PathHolder {
    public static final OpCode OPCODE = OpCode.SYNC;
    
    public ISyncResponse() {
        super();
    }

    public ISyncResponse(String path) {
        super(path);
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Responses.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Responses.TAG);
    }
}