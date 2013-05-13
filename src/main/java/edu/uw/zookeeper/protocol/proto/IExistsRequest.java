package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.ExistsRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.DataRecord;
import edu.uw.zookeeper.protocol.proto.Records.PathHolder;
import edu.uw.zookeeper.protocol.proto.Records.RequestRecord;
import edu.uw.zookeeper.protocol.proto.Records.Requests;

public class IExistsRequest extends ExistsRequest implements RequestRecord, DataRecord, PathHolder {
    public static final OpCode OPCODE = OpCode.EXISTS;
    
    public IExistsRequest() {
        super();
    }

    public IExistsRequest(String path, boolean watch) {
        super(path, watch);
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