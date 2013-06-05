package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.GetChildrenRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class IGetChildrenRequest extends GetChildrenRequest implements RequestRecord, Records.PathRecord, Records.WatchRecord {
    public static final OpCode OPCODE = OpCode.GET_CHILDREN;
    
    public IGetChildrenRequest() {
        super();
    }

    public IGetChildrenRequest(String path, boolean watch) {
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