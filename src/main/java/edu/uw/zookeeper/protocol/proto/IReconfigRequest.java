package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.ReconfigRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class IReconfigRequest extends ReconfigRequest implements RequestRecord {
    public static final OpCode OPCODE = OpCode.RECONFIG;
    
    public IReconfigRequest() {
        super();
    }

    public IReconfigRequest(String joiningServers, String leavingServers,
            String newMembers, long curConfigId) {
        super(joiningServers, leavingServers, newMembers, curConfigId);
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
