package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.protocol.proto.Records;


public interface Message extends Encodable {
    
    /**
     * Sent by client.
     */
    public static interface Client extends Message {}
    
    /**
     * Sent by server.
     */
    public static interface Server extends Message {}
    
    /**
     * Not associated with a session.
     */
    public static interface Anonymous extends Message {}

    /**
     * Associated with a session.
     */
    public static interface Session extends Message {}

    public static interface ClientSession extends Session, Client, Operation.Request {}

    public static interface ServerSession extends Session, Server, Operation.Response {}
    
    public static interface ClientRequest<T extends Records.Request> extends ClientSession, Operation.ProtocolRequest<T> {}
    
    public static interface ServerResponse<T extends Records.Response> extends ServerSession, Operation.ProtocolResponse<T> {}
}
