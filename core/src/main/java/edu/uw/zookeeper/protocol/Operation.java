package edu.uw.zookeeper.protocol;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public interface Operation {

    public static interface RequestId {
        int xid();
    }

    public static interface ResponseId {
        long zxid();
    }
    
    public static interface Coded {
        OpCode opcode();
    }
    
    public static interface RecordHolder<T extends Record> {
        T record();
    }

    public static interface Request extends Operation {}

    public static interface Response extends Operation {}

    /**
     * A response indicating an unsuccessful operation.
     */
    public static interface Error extends Response {
        KeeperException.Code error();
    }
    
    public static interface ProtocolRequest<T extends Records.Request> extends Request, RequestId, RecordHolder<T> {
    }
    
    public static interface ProtocolResponse<T extends Records.Response> extends Response, RequestId, ResponseId, RecordHolder<T> {
    }
}
