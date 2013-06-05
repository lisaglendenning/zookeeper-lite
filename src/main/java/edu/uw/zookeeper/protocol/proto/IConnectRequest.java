package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.ConnectRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.ConnectRecord;
import edu.uw.zookeeper.protocol.Records.RequestRecord;

public class IConnectRequest extends ConnectRequest implements RequestRecord, ConnectRecord {
    public static final OpCode OPCODE = OpCode.CREATE_SESSION;
    
    public IConnectRequest() {
        super();
    }

    public IConnectRequest(int protocolVersion, long lastZxidSeen, int timeOut,
            long sessionId, byte[] passwd) {
        super(protocolVersion, lastZxidSeen, timeOut, sessionId, passwd);
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