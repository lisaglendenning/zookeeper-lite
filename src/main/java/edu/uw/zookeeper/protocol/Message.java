package edu.uw.zookeeper.protocol;



public interface Message extends Encodable {
    
    /**
     * Sent by client.
     */
    public static interface ClientMessage extends Message {}
    
    /**
     * Sent by server.
     */
    public static interface ServerMessage extends Message {}

    /**
     * Associated with a Session.
     */
    public static interface SessionMessage extends Message {}

    public static interface ClientSessionMessage extends SessionMessage, ClientMessage {}

    public static interface ServerSessionMessage extends SessionMessage, ServerMessage {}
}
