package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;

public class SimpleServerBuilder implements ZooKeeperApplication.RuntimeBuilder<List<Service>, SimpleServerBuilder> {

    public static SimpleServerBuilder defaults() {
        return new SimpleServerBuilder(
                SimpleServerExecutor.builder(), ServerConnectionsHandler.builder());
    }
    
    protected final ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> server;
    protected final ServerConnectionsHandler.Builder connections;
    
    public SimpleServerBuilder(
            ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> server,
            ServerConnectionsHandler.Builder connections) {
        this.server = checkNotNull(server);
        this.connections = checkNotNull(connections);
    }

    @Override
    public RuntimeModule getRuntimeModule() {
        return server.getRuntimeModule();
    }

    @Override
    public SimpleServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return newInstance(server.setRuntimeModule(runtime), connections.setRuntimeModule(runtime));
    }

    public ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> getServerBuilder() {
        return server;
    }
    
    public SimpleServerBuilder setServerBuilder(ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> server) {
        return newInstance(server, connections);
    }
 
    public ServerConnectionsHandler.Builder getConnectionsBuilder() {
        return connections;
    }

    public SimpleServerBuilder setConnectionsBuilder(ServerConnectionsHandler.Builder connections) {
        return newInstance(server, connections);
    }
 
    @Override
    public SimpleServerBuilder setDefaults() {
        ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> server = getDefaultServerBuilder();
        if (server != this.server) {
            return setServerBuilder(server).setDefaults();
        }
        ServerConnectionsHandler.Builder connections = getDefaultConenctionsBuilder();
        if (connections != this.connections) {
            return setConnectionsBuilder(connections).setDefaults();
        }
        return this;
    }

    @Override
    public List<Service> build() {
        return setDefaults().doBuild();
    }

    protected SimpleServerBuilder newInstance(
            ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> server,
            ServerConnectionsHandler.Builder connections) {
        return new SimpleServerBuilder(server, connections);
    }

    protected List<Service> doBuild() {
        return getConnectionsBuilder().build();
    }
    
    protected ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor, ?> getDefaultServerBuilder() {
        return getServerBuilder().setDefaults();
    }
    
    protected ServerExecutor getDefaultServerExecutor() {
        return getServerBuilder().build();
    }
    
    protected ServerConnectionsHandler.Builder getDefaultConenctionsBuilder() {
        ServerConnectionsHandler.Builder connections = getConnectionsBuilder();
        if (connections.getServerExecutor() == null) {
            connections = connections.setServerExecutor(getDefaultServerExecutor());
        }
        return connections.setDefaults();
    }
}
