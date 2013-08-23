package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.Records;

public enum PingProcessor implements Processor<Object, IPingResponse> {
    PING_PROCESSOR;
    
    public static PingProcessor getInstance() {
        return PING_PROCESSOR;
    }
    
    @Override
    public IPingResponse apply(Object input) {
        return Records.newInstance(IPingResponse.class);
    }
}
