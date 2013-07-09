package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.AuthPacket;

@Operational(opcode=OpCode.AUTH)
@OperationalXid(xid=OpCodeXid.AUTH)
public class IAuthRequest extends IOpCodeXidRecord<AuthPacket> implements Records.Request {

    public IAuthRequest() {
        this(new AuthPacket());
    }
    
    public IAuthRequest(int type, String scheme, byte[] auth) {
        this(new AuthPacket(type, scheme, auth));
    }
    
    public IAuthRequest(AuthPacket record) {
        super(record);
    }

    public int getType() {
        return get().getType();
    }

    public String getScheme() {
        return get().getScheme();
    }

    public byte[] getAuth() {
        return get().getAuth();
    }
}