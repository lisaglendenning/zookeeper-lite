package edu.uw.zookeeper.server;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.util.Processor;

public class RequestErrorProcessor implements
        Processor<Operation.Request, Operation.Response> {

    public static RequestErrorProcessor newInstance(
            Processor<Operation.Request, Operation.Response> processor) {
        return new RequestErrorProcessor(processor);
    }

    protected final Processor<Operation.Request, Operation.Response> processor;

    protected RequestErrorProcessor(
            Processor<Operation.Request, Operation.Response> processor) {
        this.processor = processor;
    }

    @Override
    public Operation.Response apply(Operation.Request request) throws Exception {
        Operation.Response reply;
        try {
            reply = processor.apply(request);
        } catch (Exception e) {
            KeeperException.Code code = null;
            if (e instanceof KeeperException) {
                code = ((KeeperException)e).code();
            } else if (e instanceof IllegalArgumentException ||
                    e instanceof IllegalStateException) {
                code = KeeperException.Code.BADARGUMENTS;
            } else {
                throw e;
            }
            reply = new IErrorResponse(code);
        }
        return reply;
    }
}
