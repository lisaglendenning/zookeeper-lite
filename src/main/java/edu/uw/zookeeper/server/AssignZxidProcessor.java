package edu.uw.zookeeper.server;


import com.google.inject.Inject;

import edu.uw.zookeeper.Zxid;
import edu.uw.zookeeper.data.OpCallResponse;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.FilteredProcessor;
import edu.uw.zookeeper.util.FilteringProcessor;
import edu.uw.zookeeper.util.Processor;

public class AssignZxidProcessor implements
        Processor<Operation.Response, Operation.Response> {

    public static FilteringProcessor<Operation.Response, Operation.Response> create(
            Zxid zxid) {
        return FilteredProcessor.create(OpRequestProcessor.NotEqualsFilter
                .create(Operation.CREATE_SESSION),
                new AssignZxidProcessor(zxid));
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
