package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;

public class SimpleServerBuilder<T extends ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor<?>, ?>> extends ZooKeeperApplication.ForwardingBuilder<List<Service>, T, SimpleServerBuilder<T>> {

    public static SimpleServerBuilder<?> defaults() {
        return fromConnections(ServerConnectionsHandler.builder());
    }

    public static SimpleServerBuilder<SimpleServerExecutor.Builder> fromConnections(
            ServerConnectionsHandler.Builder connections) {
        return fromBuilders(
                SimpleServerExecutor.builder(connections.getConnectionBuilder()), 
                connections);
    }
    
    public static <T extends ZooKeeperApplication.RuntimeBuilder<? extends ServerExecutor<?>, ?>> SimpleServerBuilder<T> fromBuilders(
            T server,
            ServerConnectionsHandler.Builder connections) {
        return new SimpleServerBuilder<T>(server, connections);
    }
    
    protected final ServerConnectionsHandler.Builder connections;
    
    public SimpleServerBuilder(
            T delegate,
            ServerConnectionsHandler.Builder connections) {
        super(delegate);
        this.connections = checkNotNull(connections);
    }

    @SuppressWarnings("unchecked")
    @Override
    public SimpleServerBuilder<T> setRuntimeModule(RuntimeModule runtime) {
        if (getRuntimeModule() == runtime) {
            return this;
        } else {
            return newInstance(
                    (T) delegate.setRuntimeModule(runtime), 
                    connections.setRuntimeModule(runtime));
        }
    }

    public T getServerBuilder() {
        return delegate;
    }
    
    public SimpleServerBuilder<T> setServerBuilder(T server) {
        if (getServerBuilder() == server) {
            return this;
        } else {
            return newInstance(server, connections);
        }
    }
 
    public ServerConnectionsHandler.Builder getConnectionsBuilder() {
        return connections;
    }

    public SimpleServerBuilder<T> setConnectionsBuilder(ServerConnectionsHandler.Builder connections) {
        if (getConnectionsBuilder() == connections) {
            return this;
        } else {
            return newInstance(delegate, connections);
        }
    }
 
    @Override
    public SimpleServerBuilder<T> setDefaults() {
        checkState(getRuntimeModule() != null);
        
        T server = getDefaultServerBuilder();
        if (server != getServerBuilder()) {
            return setServerBuilder(server).setDefaults();
        }
        ServerConnectionsHandler.Builder connections = getDefaultConnectionsBuilder();
        if (connections != getConnectionsBuilder()) {
            return setConnectionsBuilder(connections).setDefaults();
        }
        return this;
    }

    @Override
    public List<Service> build() {
        return setDefaults().doBuild();
    }

    @Override
    protected SimpleServerBuilder<T> newInstance(
            T server) {
        return newInstance(server, connections);
    }
    
    protected SimpleServerBuilder<T> newInstance(
            T server,
            ServerConnectionsHandler.Builder connections) {
        return new SimpleServerBuilder<T>(server, connections);
    }

    protected List<Service> doBuild() {
        return getConnectionsBuilder().build();
    }
    
    @SuppressWarnings("unchecked")
    protected T getDefaultServerBuilder() {
        return (T) getServerBuilder().setDefaults();
    }
    
    protected ServerExecutor<?> getDefaultServerExecutor() {
        return getServerBuilder().build();
    }
    
    protected ServerConnectionsHandler.Builder getDefaultConnectionsBuilder() {
        ServerConnectionsHandler.Builder connections = getConnectionsBuilder();
        if (connections.getServerExecutor() == null) {
            connections = connections.setServerExecutor(getDefaultServerExecutor());
        }
        return connections.setDefaults();
    }
}
