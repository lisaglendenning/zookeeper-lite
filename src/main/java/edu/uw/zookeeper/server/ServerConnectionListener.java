package edu.uw.zookeeper.server;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.ServerMessageExecutor;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ServerConnectionListener<C extends Connection<Message.Server>> {
    
    public static <C extends Connection<Message.Server>> ServerConnectionListener<C> newInstance(
            final ServerConnectionFactory<Message.Server, C> connections,
            final ServerMessageExecutor anonymousExecutor,
            final ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            final Executor executor) {
        ParameterizedFactory<C, ServerConnectionExecutor<C>> serverFactory =
                new ParameterizedFactory<C, ServerConnectionExecutor<C>>() {
                    @Override
                    public ServerConnectionExecutor<C> get(C value) {
                        return ServerConnectionExecutor.newInstance(
                                value, 
                                anonymousExecutor, 
                                sessionExecutors, 
                                executor);
                    }
                    
                };
        return new ServerConnectionListener<C>(connections, serverFactory);
    }
    
    protected class ConnectionListener {
        protected final C connection;
        protected final ServerConnectionExecutor<C> server;
        
        public ConnectionListener(C connection) {
            this.connection = connection;
            this.server = serverFactory.get(connection);
            if (servers.putIfAbsent(connection, server) != null) {
                throw new AssertionError();
            }
            
            connection.register(this);
        }

        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}
                servers.remove(connection, server);
            }
        }
    }

    protected final ServerConnectionFactory<Message.Server, C> connections;
    protected final ParameterizedFactory<C, ServerConnectionExecutor<C>> serverFactory;
    protected final ConcurrentMap<C, ServerConnectionExecutor<C>> servers;
    
    public ServerConnectionListener(
            ServerConnectionFactory<Message.Server, C> connections,
            ParameterizedFactory<C, ServerConnectionExecutor<C>> serverFactory) {
        this.connections = connections;
        this.serverFactory = serverFactory;
        this.servers = Maps.newConcurrentMap();
        
        connections.register(this);
    }
    
    @Subscribe
    public void handleNewConnection(C connection) {
        new ConnectionListener(connection);
    }
}
