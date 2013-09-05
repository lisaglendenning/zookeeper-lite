package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.AuthPacket;

@Operational(value=OpCode.AUTH)
@OperationalXid(value=OpCodeXid.AUTH)
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
        return record.getType();
    }

    public String getScheme() {
        return record.getScheme();
    }

    public byte[] getAuth() {
        return record.getAuth();
    }
}