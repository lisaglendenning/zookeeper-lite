package edu.uw.zookeeper.protocol;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.proto.OpCode;

public abstract class Operation {

    public static interface XidHeader {
        int xid();
    }

    public static interface ZxidHeader {
        long zxid();
    }
    
    public static interface ClientRequest {}
    
    public static interface ServerResponse {}
    
    public static interface Action extends Record {
        OpCode opcode();
    }

    public static interface Request extends Action, ClientRequest {}

    public static interface Response extends Action, ServerResponse {}

    /**
     * A reply indicating an unsuccessful operation.
     */
    public static interface Error extends Response {
        KeeperException.Code error();
    }
    
    public static interface SessionRequest extends Message.ClientSessionMessage, XidHeader, ClientRequest {
        Request request();
    }
    
    public static interface SessionReply extends Message.ServerSessionMessage, XidHeader, ZxidHeader, ServerResponse {
        Response reply();
    }
    
    public static interface SessionResult {
        SessionRequest request();
        SessionReply reply();
    }
}
