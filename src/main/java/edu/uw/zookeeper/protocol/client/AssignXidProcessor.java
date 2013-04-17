package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.XidCounter;
import edu.uw.zookeeper.data.OpCallRequest;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Processor;

public class AssignXidProcessor implements
        Processor<Operation.Request, Operation.Request> {

    public static AssignXidProcessor create() {
        return new AssignXidProcessor(XidCounter.create());
    }

    public static AssignXidProcessor create(XidCounter xid) {
        return new AssignXidProcessor(xid);
    }

    protected final XidCounter xid;

    protected AssignXidProcessor(XidCounter xid) {
        this.xid = xid;
    }

    public XidCounter xid() {
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
