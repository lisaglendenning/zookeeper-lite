package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ConnectRequest;

@Operational(value=OpCode.CREATE_SESSION)
public class IConnectRequest extends IOperationalRecord<ConnectRequest> implements Records.Request, Records.ConnectGetter {

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
        return record.getProtocolVersion();
    }

    @Override
    public int getTimeOut() {
        return record.getTimeOut();
    }

    @Override
    public long getSessionId() {
        return record.getSessionId();
    }

    @Override
    public byte[] getPasswd() {
        return record.getPasswd();
    }
    
    public long getLastZxidSeen() {
        return record.getLastZxidSeen();
    }
}