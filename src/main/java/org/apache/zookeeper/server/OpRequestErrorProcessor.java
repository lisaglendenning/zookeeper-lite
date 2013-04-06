package org.apache.zookeeper.server;

import org.apache.zookeeper.data.Operation;

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
