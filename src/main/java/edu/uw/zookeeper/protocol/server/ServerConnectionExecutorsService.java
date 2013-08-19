package edu.uw.zookeeper.protocol.server;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;

public class ServerConnectionExecutorsService<T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> extends ForwardingService implements Iterable<ServerConnectionExecutor<T>> {

    public static <T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> ServerConnectionExecutorsService<T> newInstance(
            ServerConnectionFactory<T> connections,
            ServerTaskExecutor server) {
        return new ServerConnectionExecutorsService<T>(
                connections, 
                ServerConnectionExecutor.<T>factory(
                        server.getAnonymousExecutor(), 
                        server.getConnectExecutor(), 
                        server.getSessionExecutor()));
    }

    protected final ServerConnectionFactory<T> connections;
    protected final ParameterizedFactory<T, ServerConnectionExecutor<T>> factory;
    protected final ConcurrentMap<T, ServerConnectionExecutor<T>> handlers;
    
    public ServerConnectionExecutorsService(
            ServerConnectionFactory<T> connections,
            ParameterizedFactory<T, ServerConnectionExecutor<T>> factory) {
        this.connections = connections;
        this.factory = factory;
        this.handlers = new MapMaker().makeMap();
    }
    
    @Override
    public Iterator<ServerConnectionExecutor<T>> iterator() {
        return handlers.values().iterator();
    }

    public ServerConnectionFactory<T> connections() {
        return connections;
    }

    @Subscribe
    public void handleNewConnection(T connection) {
        new RemoveOnClose(connection, factory.get(connection));
    }

    @Override
    protected Service delegate() {
        return connections;
    }

    @Override
    protected void startUp() throws Exception {
        connections.register(this);
        
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        
        try {
            connections.unregister(this);
        } catch (IllegalArgumentException e) {}
    }

    protected class RemoveOnClose extends Pair<T, ServerConnectionExecutor<T>> {
        
        public RemoveOnClose(T connection, ServerConnectionExecutor<T> handler) {
            super(connection, handler);
            if (handlers.putIfAbsent(connection, handler) != null) {
                throw new AssertionError();
            }
            connection.register(this);
        }
    
        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    first().unregister(this);
                } catch (IllegalArgumentException e) {}
                handlers.remove(first(), second());
            }
        }
    }
}
