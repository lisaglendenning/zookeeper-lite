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
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class Server {
    
    public static Server newInstance(
            final Factory<Publisher> publisherFactory,
            final ServerConnectionFactory<Message.ServerMessage, ? extends Connection<Message.ServerMessage>> connections,
            final ServerExecutor serverExecutor) {
        ParameterizedFactory<Connection<Message.ServerMessage>, ServerCodecConnection> codecFactory = ServerCodecConnection.factory(publisherFactory);
        ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor> protocolFactory =
                new ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor>() {
                    @Override
                    public ServerProtocolExecutor get(
                            ServerCodecConnection value) {
                        ServerProtocolExecutor server = ServerProtocolExecutor.newInstance(value, serverExecutor, serverExecutor, serverExecutor.executor());
                        return server;
                    }
                    
                };
        return new Server(publisherFactory, connections, serverExecutor, Factories.linkParameterized(codecFactory, protocolFactory));
    }
    
    protected class ConnectionListener {
        protected final Connection<Message.ServerMessage> connection;
        protected final ServerProtocolExecutor server;
        
        public ConnectionListener(Connection<Message.ServerMessage> connection) {
            this.connection = connection;
            this.server = serverFactory.get(connection);
            if (servers.putIfAbsent(connection, server) != null) {
                throw new AssertionError();
            }
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                servers.remove(connection, server);
            }
        }
    }

    protected final ServerConnectionFactory<Message.ServerMessage, ? extends Connection<Message.ServerMessage>> connections;
    protected final ServerExecutor serverExecutor;
    protected final ParameterizedFactory<Connection<Message.ServerMessage>, ServerProtocolExecutor> serverFactory;
    protected final ConcurrentMap<Connection<Message.ServerMessage>, ServerProtocolExecutor> servers;
    
    protected Server(
            final Factory<Publisher> publisherFactory,
            final ServerConnectionFactory<Message.ServerMessage, ? extends Connection<Message.ServerMessage>> connections,
            final ServerExecutor serverExecutor,
            ParameterizedFactory<Connection<Message.ServerMessage>, ServerProtocolExecutor> serverFactory) {
        this.connections = connections;
        this.serverExecutor = serverExecutor;
        this.serverFactory = serverFactory;
        this.servers = Maps.newConcurrentMap();
        
        connections.register(this);
    }
    
    @Subscribe
    public void handleNewConnection(Connection<Message.ServerMessage> event) {
        new ConnectionListener(event);
    }
}
