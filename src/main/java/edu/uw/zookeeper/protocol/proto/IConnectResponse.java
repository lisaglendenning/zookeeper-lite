package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.ConnectResponse;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.proto.Records.ConnectRecord;
import edu.uw.zookeeper.protocol.proto.Records.ResponseRecord;

public class IConnectResponse extends ConnectResponse implements ResponseRecord, ConnectRecord {
    public static final OpCode OPCODE = OpCode.CREATE_SESSION;
    
    public IConnectResponse() {
        super();
    }

    public IConnectResponse(int protocolVersion, int timeOut, long sessionId,
            byte[] passwd) {
        super(protocolVersion, timeOut, sessionId, passwd);
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Records.CONNECT_TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Records.CONNECT_TAG);
    }
}