package edu.uw.zookeeper.server;


import com.google.inject.Inject;

import edu.uw.zookeeper.ZxidCounter;
import edu.uw.zookeeper.data.OpCallResponse;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.FilteredProcessor;
import edu.uw.zookeeper.util.FilteringProcessor;
import edu.uw.zookeeper.util.Processor;

public class AssignZxidProcessor implements
        Processor<Operation.Response, Operation.Response> {

    public static FilteringProcessor<Operation.Response, Operation.Response> create(
            ZxidCounter zxid) {
        return FilteredProcessor.create(OpRequestProcessor.NotEqualsFilter
                .create(Operation.CREATE_SESSION),
                new AssignZxidProcessor(zxid));
    }

    protected final ZxidCounter zxid;

    @Inject
    protected AssignZxidProcessor(ZxidCounter zxid) {
        this.zxid = zxid;
    }

    public ZxidCounter zxid() {
        return zxid;
    }

    @Override
    public Operation.Response apply(Operation.Response input) throws Exception {
        Operation.Response output;
        // FIXME: do error'ed responses get a zxid?
        switch (input.operation()) {
        case CREATE_SESSION:
            output = input;
            break;
        case PING:
        case AUTH: {
            long zxid = zxid().get();
            output = OpCallResponse.create(zxid, input);
            break;
        }
        default: {
            long zxid = zxid().incrementAndGet();
            output = OpCallResponse.create(zxid, input);
            break;
        }
        }
        return output;
    }
}
