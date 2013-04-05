package org.apache.zookeeper.server;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.OpCallResponse;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.util.FilteredProcessor;
import org.apache.zookeeper.util.FilteringProcessor;
import org.apache.zookeeper.util.Processor;

import com.google.inject.Inject;

public class AssignZxidProcessor implements Processor<Operation.Response, Operation.Response> {

    public static FilteringProcessor<Operation.Response, Operation.Response> create(
            Zxid zxid) {
        return FilteredProcessor.create(OpRequestProcessor.NotEqualsFilter.create(Operation.CREATE_SESSION),
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
        case AUTH:
        {
            long zxid = zxid().get();
            output = OpCallResponse.create(zxid, input);
            break;
        }
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
