package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.GetChildren2Request;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.DataRecord;
import edu.uw.zookeeper.protocol.proto.Records.PathHolder;
import edu.uw.zookeeper.protocol.proto.Records.RequestRecord;
import edu.uw.zookeeper.protocol.proto.Records.Requests;

public class IGetChildren2Request extends GetChildren2Request implements RequestRecord, DataRecord, PathHolder {
    public static final OpCode OPCODE = OpCode.GET_CHILDREN2;
    
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