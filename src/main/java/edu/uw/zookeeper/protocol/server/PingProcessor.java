package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.protocol.Ping;
import edu.uw.zookeeper.util.Processor;

public enum PingProcessor implements Processor<Ping.Request, Ping.Response> {
    PING_PROCESSOR;
    
    public static PingProcessor getInstance() {
        return PING_PROCESSOR;
    }
    
    @Override
    public Ping.Response apply(Ping.Request input) {
        return Ping.Response.newInstance();
    }
}
