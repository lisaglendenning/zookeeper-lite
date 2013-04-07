package org.apache.zookeeper.protocol.client;

import org.apache.zookeeper.Xid;
import org.apache.zookeeper.data.OpCallRequest;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.util.Processor;

public class AssignXidProcessor implements
        Processor<Operation.Request, Operation.Request> {

    public static AssignXidProcessor create() {
        return new AssignXidProcessor(Xid.create());
    }

    public static AssignXidProcessor create(Xid xid) {
        return new AssignXidProcessor(xid);
    }

    protected final Xid xid;

    protected AssignXidProcessor(Xid xid) {
        this.xid = xid;
    }

    public Xid xid() {
        return xid;
    }

    @Override
    public Operation.Request apply(Operation.Request request) {
        if ((request.operation() != Operation.CREATE_SESSION)
                && !(request instanceof Operation.CallRequest)) {
            int xid = xid().incrementAndGet();
            request = OpCallRequest.create(xid, request);
        }
        return request;
    }
}
