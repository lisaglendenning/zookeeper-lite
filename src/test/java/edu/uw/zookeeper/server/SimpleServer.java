package edu.uw.zookeeper.server;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.server.SimpleServerConnections;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleServer extends ForwardingService {

    public static SimpleServer create() {
        SimpleServerExecutor executor = SimpleServerExecutor.newInstance();
        SimpleServerConnections connections = SimpleServerConnections.newInstance(executor.getTasks());
        return new SimpleServer(connections, executor);
    }
    
    protected final SimpleServerConnections connections;
    protected final SimpleServerExecutor executor;
    
    protected SimpleServer(
            SimpleServerConnections connections,
            SimpleServerExecutor executor) {
        this.connections = connections;
        this.executor = executor;
    }
    
    public SimpleServerConnections getConnections() {
        return connections;
    }
    
    public SimpleServerExecutor getExecutor() {
        return executor;
    }

    @Override
    protected Service delegate() {
        return connections;
    }
}
