package edu.uw.zookeeper.server;

import edu.uw.zookeeper.data.OpResult;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Processor;

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
