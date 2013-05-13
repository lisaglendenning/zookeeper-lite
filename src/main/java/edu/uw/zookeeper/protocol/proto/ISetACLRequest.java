package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.SetACLRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.DataRecord;
import edu.uw.zookeeper.protocol.proto.Records.PathHolder;
import edu.uw.zookeeper.protocol.proto.Records.RequestRecord;
import edu.uw.zookeeper.protocol.proto.Records.Requests;

public class ISetACLRequest extends SetACLRequest implements RequestRecord, DataRecord, PathHolder {
    public static final OpCode OPCODE = OpCode.SET_ACL;
    
    public ISetACLRequest() {
        super();
    }

    public ISetACLRequest(String path, List<ACL> acl, int version) {
        super(path, acl, version);
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