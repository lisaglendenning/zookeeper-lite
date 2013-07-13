package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ConnectRequest;

@Operational(value=OpCode.CREATE_SESSION)
public class IConnectRequest extends ICodedRecord<ConnectRequest> implements Records.Request, Records.ConnectGetter {

    public IConnectRequest() {
        this(new ConnectRequest());
    }
    
    public IConnectRequest(int protocolVersion, long lastZxidSeen, int timeOut,
            long sessionId, byte[] passwd) {
        this(new ConnectRequest(protocolVersion, lastZxidSeen, timeOut, sessionId, passwd));
    }

    public IConnectRequest(ConnectRequest record) {
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
    
    public long getLastZxidSeen() {
        return get().getLastZxidSeen();
    }
}