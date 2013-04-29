package edu.uw.zookeeper.server;

import edu.uw.zookeeper.protocol.Operation;

public class OpRequestErrorProcessor extends OpRequestProcessor {

    public static OpRequestErrorProcessor create() {
        return new OpRequestErrorProcessor();
    }

    protected OpRequestErrorProcessor() {
    }

    @Override
    public Operation.Response apply(Operation.Request request) throws Exception {
        throw new IllegalArgumentException(request.toString());
    }
}
