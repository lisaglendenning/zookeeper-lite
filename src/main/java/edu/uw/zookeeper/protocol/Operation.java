package edu.uw.zookeeper.protocol;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;

public abstract class Operation {

    public static interface XidHeader {
        int xid();
    }

    public static interface ZxidHeader {
        long zxid();
    }
    
    public static interface Action {
        OpCode opcode();
    }
    
    public static interface RecordHolder<T extends Record> {
        T asRecord();
    }

    public static interface Request extends Action {}

    public static interface Reply {}

    /**
     * A reply indicating a successful operation.
     */
    public static interface Response extends Action, Reply {}

    /**
     * A reply indicating an unsuccessful operation.
     */
    public static interface Error extends Reply {
        KeeperException.Code error();
    }
    
    public static interface SessionRequest extends Message.ClientSessionMessage, XidHeader {
        Request request();
    }
    
    public static interface SessionReply extends Message.ServerSessionMessage, XidHeader, ZxidHeader {
        Reply reply();
    }
    
    public static interface SessionResult {
        SessionRequest request();
        SessionReply reply();
    }
}
