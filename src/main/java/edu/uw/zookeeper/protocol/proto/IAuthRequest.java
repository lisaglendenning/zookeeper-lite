package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.AuthPacket;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.AUTH)
public class IAuthRequest extends IOperationalXidRecord<AuthPacket> implements Operation.Request, Operation.XidHeader {

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