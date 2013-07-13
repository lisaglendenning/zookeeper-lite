package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.Ping;

public enum PingProcessor implements TxnRequestProcessor<Ping.Request, Ping.Response> {
    PING_PROCESSOR;
    
    public static PingProcessor getInstance() {
        return PING_PROCESSOR;
    }
    
    @Override
    public Ping.Response apply(TxnOperation.Request<Ping.Request> input) {
        return Ping.Response.newInstance();
    }
}
