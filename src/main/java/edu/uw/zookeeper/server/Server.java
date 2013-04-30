package edu.uw.zookeeper.server;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.event.NewConnectionEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class Server {
    
    public static Server newInstance(
            Factory<Publisher> publisherFactory,
            ServerConnectionFactory connections,
            ServerExecutor serverExecutor) {
        return new Server(publisherFactory, connections, serverExecutor);
    }

    protected final ServerConnectionFactory connections;
    protected final ServerExecutor serverExecutor;
    protected final ParameterizedFactory<Connection, ServerProtocolConnection> serverFactory;
    protected final ConcurrentMap<Connection, ServerProtocolConnection> servers;
    
    protected Server(
            final Factory<Publisher> publisherFactory,
            final ServerConnectionFactory connections,
            final ServerExecutor serverExecutor) {
        this.connections = connections;
        this.serverExecutor = serverExecutor;
        ParameterizedFactory<Connection, ServerCodecConnection> codecFactory = ServerCodecConnection.factory(publisherFactory);
        ParameterizedFactory<ServerCodecConnection, ServerProtocolConnection> protocolFactory =
                new ParameterizedFactory<ServerCodecConnection, ServerProtocolConnection>() {
                    @Override
                    public ServerProtocolConnection get(
                            ServerCodecConnection value) {
                        return ServerProtocolConnection.newInstance(value, serverExecutor, serverExecutor, serverExecutor.executor());
                    }
                    
                };
        this.serverFactory = Factories.linkParameterized(codecFactory, protocolFactory);
        this.servers = Maps.newConcurrentMap();
        
        connections.register(this);
    }
    
    @Subscribe
    public void handleNewConnection(NewConnectionEvent event) {
        Connection connection = event.connection();
        ServerProtocolConnection server = serverFactory.get(connection);
        servers.put(connection, server);
        connection.register(this);
    }
    
    @Subscribe
    public void handleConnectionStateEvent(ConnectionStateEvent event) {
        Connection connection = event.connection();
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            servers.remove(connection);
            break;
        default:
            break;
        }
    }
}
