package edu.uw.zookeeper.server;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.ClientMessageExecutor;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolExecutor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ServerConnectionListener {
    
    public static ServerConnectionListener newInstance(
            final ServerConnectionFactory<Message.ServerMessage, ServerCodecConnection> connections,
            final ClientMessageExecutor anonymousExecutor,
            final ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            final Executor executor) {
        ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor> serverFactory =
                new ParameterizedFactory<ServerCodecConnection, ServerProtocolExecutor>() {
                    @Override
                    public ServerProtocolExecutor get(
                            ServerCodecConnection value) {
                        ServerProtocolExecutor server = ServerProtocolExecutor.newInstance(
                                value, 
                                anonymousExecutor, 
                                sessionExecutors, 
                                executor);
                        return server;
                    }
                    
                };
        return new ServerConnectionListener(connections, serverFactory);
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
        public void handleStateEvent(Automaton.Transition<?> event) {
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
    
    protected ServerConnectionListener(
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
