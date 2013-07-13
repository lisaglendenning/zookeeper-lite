package edu.uw.zookeeper.protocol.server;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ServerConnectionExecutorsService<C extends Connection<Message.Server>> extends AbstractIdleService {

    public static <C extends Connection<Message.Server>> ServerConnectionExecutorsService<C> newInstance(
            ServerConnectionFactory<Message.Server, C> connections,
            ServerTaskExecutor server) {
        return new ServerConnectionExecutorsService<C>(
                connections, 
                ServerConnectionExecutor.<C>factory(
                        server.getAnonymousExecutor(), 
                        server.getConnectExecutor(), 
                        server.getSessionExecutor()));
    }
    
    protected final ServerConnectionFactory<Message.Server, C> connections;
    protected final ParameterizedFactory<C, ServerConnectionExecutor<C>> factory;
    protected final ConcurrentMap<C, ServerConnectionExecutor<C>> servers;
    
    public ServerConnectionExecutorsService(
            ServerConnectionFactory<Message.Server, C> connections,
            ParameterizedFactory<C, ServerConnectionExecutor<C>> factory) {
        this.connections = connections;
        this.factory = factory;
        this.servers = new MapMaker().makeMap();
        
        connections.register(this);
    }
    
    @Subscribe
    public void handleNewConnection(C connection) {
        new ConnectionListener(connection);
    }

    protected class ConnectionListener {
        protected final C connection;
        protected final ServerConnectionExecutor<C> server;
        
        public ConnectionListener(C connection) {
            this.connection = connection;
            this.server = factory.get(connection);
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

    @Override
    protected void startUp() throws Exception {
        connections.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        connections.stop().get();
    }
}
