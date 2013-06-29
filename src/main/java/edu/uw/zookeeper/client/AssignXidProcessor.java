package edu.uw.zookeeper.client;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Processor;

public class AssignXidProcessor implements
        Processor<Operation.Request, Operation.SessionRequest>,
        Generator<Integer> {

    public static Factory<AssignXidProcessor> factory() {
        return new Factory<AssignXidProcessor>() {
            @Override
            public AssignXidProcessor get() {
                return AssignXidProcessor.newInstance();
            }
        };
    }
    
    public static AssignXidProcessor newInstance() {
        return new AssignXidProcessor(XidIncrementer.fromZero());
    }

    public static AssignXidProcessor newInstance(Generator<Integer> xid) {
        return new AssignXidProcessor(xid);
    }

    private final Generator<Integer> xids;

    private AssignXidProcessor(Generator<Integer> xids) {
        this.xids = xids;
    }

    @Override
    public SessionRequestMessage apply(Operation.Request request) {        
        int xid;
        if (request instanceof Operation.XidHeader) {
            xid = ((Operation.XidHeader)request).xid();
        } else {
            xid = next();
        }
        return SessionRequestMessage.newInstance(xid, request);
    }

    @Override
    public Integer get() {
        return xids.get();
    }

    @Override
    public Integer next() {
        return xids.next();
    }
}
