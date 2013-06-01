package edu.uw.zookeeper.server;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolExecutor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class Server {
    
    public static Server newInstance(
            final Factory<Publisher> publisherFactory,
            final ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> connections,
            final ServerExecutor serverExecutor) {
        ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor> serverFactory =
                new ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor>() {
                    @Override
                    public ServerProtocolExecutor get(
                            ServerCodecConnection value) {
                        ServerProtocolExecutor server = ServerProtocolExecutor.newInstance(
                                value, 
                                serverExecutor, 
                                serverExecutor, 
                                serverExecutor);
                        return server;
                    }
                    
                };
        return new Server(
                publisherFactory, connections, serverFactory);
    }
    
    protected class ConnectionListener {
        protected final ServerCodecConnection connection;
        protected final ServerProtocolExecutor server;
        
        public ConnectionListener(ServerCodecConnection connection) {
            this.connection = connection;
            this.server = serverFactory.get(connection);
            if (servers.put(connection, server) != null) {
                throw new AssertionError();
            }
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}
                servers.remove(connection, server);
            }
        }
    }

    protected final ServerConnectionFactory<Message.ServerMessage, ? extends Connection<Message.ServerMessage>> connections;
    protected final ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor> serverFactory;
    protected final ConcurrentMap<Connection<Message.ServerMessage>, ServerProtocolExecutor> servers;
    
    protected Server(
            Factory<Publisher> publisherFactory,
            ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> connections,
            ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor> serverFactory) {
        this.connections = connections;
        this.serverFactory = serverFactory;
        this.servers = Maps.newConcurrentMap();
        
        connections.register(this);
    }
    
    @Subscribe
    public void handleNewConnection(ServerCodecConnection event) {
        new ConnectionListener(event);
    }
}
