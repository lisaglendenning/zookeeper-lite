package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.OpAction;
import org.apache.zookeeper.data.OpError;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.util.Processor;

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
