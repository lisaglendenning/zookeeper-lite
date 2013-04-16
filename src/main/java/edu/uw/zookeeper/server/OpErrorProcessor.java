package edu.uw.zookeeper.server;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.data.OpAction;
import edu.uw.zookeeper.data.OpError;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Processor;

public class OpErrorProcessor implements
        Processor<Operation.Request, Operation.Response> {

    public static OpErrorProcessor create(
            Processor<Operation.Request, Operation.Response> processor) {
        return new OpErrorProcessor(processor);
    }

    protected Processor<Operation.Request, Operation.Response> processor;

    protected OpErrorProcessor(
            Processor<Operation.Request, Operation.Response> processor) {
        this.processor = processor;
    }

    @Override
    public Operation.Response apply(Operation.Request request) throws Exception {
        Operation.Response response;
        try {
            response = processor.apply(request);
        } catch (KeeperException e) {
            response = OpError.create(
                    OpAction.Response.create(request.operation()), e.code());
        }
        return response;
    }
}
