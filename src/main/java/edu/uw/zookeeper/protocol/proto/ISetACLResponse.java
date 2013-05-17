package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetACLResponse;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;
import edu.uw.zookeeper.protocol.proto.Records.Responses;

public class ISetACLResponse extends SetACLResponse implements ResponseRecord, Records.StatHolder {
    public static final OpCode OPCODE = OpCode.SET_ACL;
    
    public ISetACLResponse() {
        super();
    }

    public ISetACLResponse(Stat stat) {
        super(stat);
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