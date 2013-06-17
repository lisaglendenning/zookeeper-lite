package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ConnectRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CREATE_SESSION)
public class IConnectRequest extends IOperationalRecord<ConnectRequest> implements Operation.Request, Records.ConnectHolder {

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