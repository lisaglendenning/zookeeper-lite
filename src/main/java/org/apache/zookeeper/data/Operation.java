package org.apache.zookeeper.data;

import static com.google.common.base.Preconditions.*;

import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;

import com.google.common.collect.Maps;


public enum Operation {
    
    NOTIFICATION(OpCode.notification) {
    },

    CREATE(OpCode.create) {
    },

    DELETE(OpCode.delete) {
    },

    EXISTS(OpCode.exists) {
    },

    GET_DATA(OpCode.getData) {
    },

    SET_DATA(OpCode.setData) {
    },

    GET_ACL(OpCode.getACL) {
    },

    SET_ACL(OpCode.setACL) {
    },

    GET_CHILDREN(OpCode.getChildren) {
    },

    SYNC(OpCode.sync) {
    },

    PING(OpCode.ping) {
    },

    GET_CHILDREN2(OpCode.getChildren2) {
    },

    CHECK(OpCode.check) {
    },

    MULTI(OpCode.multi) {
    },

    AUTH(OpCode.auth) {
    },

    SET_WATCHES(OpCode.setWatches) {
    },

    SASL(OpCode.sasl) {
    },

    CREATE_SESSION(OpCode.createSession) {
    },

    CLOSE_SESSION(OpCode.closeSession) {
    },

    ERROR(OpCode.error) {
    };

    public static interface Action {
        Operation operation();
    }
    
    public static interface Request extends Action {
    }

    public static interface Response extends Action {
    }

    public static interface RequestValue extends Request {
        Operation.Request request();
    }

    public static interface ResponseValue extends Response {
        Operation.Response response();
    }

    public static interface Error extends Response {
        KeeperException.Code error();
    }
      
    public static interface Result extends RequestValue, ResponseValue {
    }
    
    public static interface CallRequest extends Request {
        int xid();
    }
    
    public static interface CallResponse extends ResponseValue {
        long zxid();
    }

    public static interface CallReply extends CallRequest, CallResponse {
    }
    
    public static interface CallResult extends CallReply, Result {
    }

    protected static final Map<Integer, Operation> codeToOperation = Maps.newHashMap();
    static {
        for (Operation item : Operation.values()) {
            Integer opcode = item.code();
            assert (!(codeToOperation.containsKey(opcode)));
            codeToOperation.put(opcode, item);
        }
    }

    public static Operation get(int code) {
        checkArgument(codeToOperation.containsKey(code));
        return codeToOperation.get(code);
    }

    protected final int code;

    private Operation(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
