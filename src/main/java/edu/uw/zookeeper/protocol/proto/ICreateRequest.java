package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CreateRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records.CreateRecord;
import edu.uw.zookeeper.protocol.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class ICreateRequest extends CreateRequest implements RequestRecord, CreateRecord, MultiOpRequest {
    public static final OpCode OPCODE = OpCode.CREATE;
    
    public ICreateRequest() {
        super();
    }

    public ICreateRequest(String path, byte[] data, List<ACL> acl, int flags) {
        super(path, data, acl, flags);
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
