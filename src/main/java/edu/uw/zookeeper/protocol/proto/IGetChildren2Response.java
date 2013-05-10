package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetChildren2Response;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;
import edu.uw.zookeeper.protocol.proto.Records.Responses;

public class IGetChildren2Response extends GetChildren2Response implements ResponseRecord, Records.ChildrenHolder {
    public static final OpCode OPCODE = OpCode.GET_CHILDREN2;
    
    public IGetChildren2Response() {
        super();
    }

    public IGetChildren2Response(List<String> children, Stat stat) {
        super(children, stat);
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