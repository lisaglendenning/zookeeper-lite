package edu.uw.zookeeper.server;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.OpError;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Processor;

public class OpErrorProcessor implements
        Processor<Operation.Request, Operation.Reply> {

    public static OpErrorProcessor newInstance(
            Processor<Operation.Request, Operation.Response> processor) {
        return new OpErrorProcessor(processor);
    }

    protected final Processor<Operation.Request, Operation.Response> processor;

    protected OpErrorProcessor(
            Processor<Operation.Request, Operation.Response> processor) {
        this.processor = processor;
    }

    @Override
    public Operation.Reply apply(Operation.Request request) throws Exception {
        Operation.Reply reply;
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
            reply = OpError.create(code);
        }
        return reply;
    }
}
