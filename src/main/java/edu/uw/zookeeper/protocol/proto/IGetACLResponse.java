package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetACLResponse;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.DataRecord;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;
import edu.uw.zookeeper.protocol.proto.Records.Responses;

public class IGetACLResponse extends GetACLResponse implements ResponseRecord, DataRecord {
    public static final OpCode OPCODE = OpCode.GET_ACL;
    
    public IGetACLResponse() {
        super();
    }

    public IGetACLResponse(List<ACL> acl, Stat stat) {
        super(acl, stat);
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