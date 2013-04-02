package org.apache.zookeeper.server;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.OpCallResponse;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.Processor;

import com.google.inject.Inject;

public class AssignZxidProcessor implements Processor<Operation.Response, Operation.Response> {

    public static AssignZxidProcessor create(Zxid zxid) {
        return new AssignZxidProcessor(zxid);
    }
    
    protected final Zxid zxid;
    
    @Inject
    protected AssignZxidProcessor(Zxid zxid) {
        this.zxid = zxid;
    }
    
    public Zxid zxid() {
        return zxid;
    }

    @Override
    public Operation.Response apply(Operation.Response input) throws Exception {
        Operation.Response output;
        switch (input.operation()) {
        case CREATE_SESSION:
        case PING:
        case AUTH:
            output = input;
            break;
        default:
        {
            long zxid = zxid().incrementAndGet();
            output = OpCallResponse.create(zxid, input);
            break;
        }
        }
        return output;
    }
}
