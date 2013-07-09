package edu.uw.zookeeper.protocol;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class Operation {

    public static interface RequestId {
        int xid();
    }

    public static interface ResponseId {
        long zxid();
    }
    
    public static interface Coded {
        OpCode opcode();
    }

    public static interface Request {}

    public static interface Response {}

    /**
     * A response indicating an unsuccessful operation.
     */
    public static interface Error extends Response {
        KeeperException.Code error();
    }
    
    public static interface SessionRequest extends Request, RequestId {
        Records.Request request();
    }
    
    public static interface SessionResponse extends Response, RequestId, ResponseId {
        Records.Response response();
    }

    private Operation() {}
}
