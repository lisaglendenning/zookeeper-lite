package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.SyncRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class ISyncRequest extends SyncRequest implements RequestRecord, Records.PathRecord {
    public static final OpCode OPCODE = OpCode.SYNC;
    
    public ISyncRequest() {
        super();
    }

    public ISyncRequest(String path) {
        super(path);
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Requests.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Requests.TAG);
    }
}