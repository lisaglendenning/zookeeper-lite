package org.apache.zookeeper.server;

import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.OpAction;
import org.apache.zookeeper.protocol.OpCallResponse;
import org.apache.zookeeper.protocol.OpError;
import org.apache.zookeeper.protocol.OpResult;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;

public class OpCallResponseTask implements Callable<Operation.Response>, Operation.RequestValue {

    public static OpCallResponseTask create(Zxid zxid, OpRequestTask request) {
        return new OpCallResponseTask(zxid, request);
    }
    
    protected Zxid zxid;
    protected OpRequestTask request;
    
    public OpCallResponseTask(Zxid zxid, OpRequestTask request) {
        this.zxid = zxid;
        this.request = request;
    }
    
    protected Zxid zxid() {
        return zxid;
    }
    
    @Override
    public Operation.Response call() throws Exception {
        // FIXME: do error'ed responses get a zxid?
        Operation.Response response;
        try {
            response = request.call();
        } catch (KeeperException e) {
            response = OpError.create(OpAction.Response.create(operation()), e.code());
        }
        switch (operation()) {
        case CREATE_SESSION:
            return response;
        case PING:
        case AUTH:
        {
            long zxid = zxid().get();
            response = OpCallResponse.create(zxid, response);
            break;
        }
        default:
        {
            long zxid = zxid().incrementAndGet();
            response = OpCallResponse.create(zxid, response);
            break;
        }
        }

        return response;
    }

    @Override
    public Operation operation() {
        return request.operation();
    }

    @Override
    public Operation.Request request() {
        return request.request();
    }
}
