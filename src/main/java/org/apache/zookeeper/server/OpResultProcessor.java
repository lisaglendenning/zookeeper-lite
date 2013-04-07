package org.apache.zookeeper.server;

import org.apache.zookeeper.data.OpResult;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.util.Processor;

public class OpResultProcessor implements
        Processor<Operation.Request, Operation.Result> {

    public static OpResultProcessor create(
            Processor<Operation.Request, Operation.Response> processor) {
        return new OpResultProcessor(processor);
    }

    protected Processor<Operation.Request, Operation.Response> processor;

    public OpResultProcessor(
            Processor<Operation.Request, Operation.Response> processor) {
        this.processor = processor;
    }

    @Override
    public Operation.Result apply(Operation.Request request) throws Exception {
        Operation.Response response = processor.apply(request);
        Operation.Result result = OpResult.create(request, response);
        return result;
    }
}
