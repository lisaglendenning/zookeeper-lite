package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ConnectResponse;

@Operational(value=OpCode.CREATE_SESSION)
public class IConnectResponse extends ICodedRecord<ConnectResponse> implements Records.Response, Records.ConnectGetter {

    public IConnectResponse() {
        this(new ConnectResponse());
    }
    
    public IConnectResponse(int protocolVersion, int timeOut, long sessionId,
            byte[] passwd) {
        this(new ConnectResponse(protocolVersion, timeOut, sessionId, passwd));
    }

    public IConnectResponse(ConnectResponse record) {
        super(record);
    }

    @Override
    public int getProtocolVersion() {
        return get().getProtocolVersion();
    }

    @Override
    public int getTimeOut() {
        return get().getTimeOut();
    }

    @Override
    public long getSessionId() {
        return get().getSessionId();
    }

    @Override
    public byte[] getPasswd() {
        return get().getPasswd();
    }
}