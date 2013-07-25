package edu.uw.zookeeper.protocol.server;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ServerConnectionExecutorsService<C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> extends AbstractIdleService implements Iterable<ServerConnectionExecutor<C,T>> {

    public static <C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> ServerConnectionExecutorsService<C,T> newInstance(
            ServerConnectionFactory<Message.Server, T> connections,
            ServerTaskExecutor server) {
        return new ServerConnectionExecutorsService<C,T>(
                connections, 
                ServerConnectionExecutor.<C,T>factory(
                        server.getAnonymousExecutor(), 
                        server.getConnectExecutor(), 
                        server.getSessionExecutor()));
    }

    protected final ServerConnectionFactory<Message.Server, T> connections;
    protected final ParameterizedFactory<T, ServerConnectionExecutor<C,T>> factory;
    protected final ConcurrentMap<T, ServerConnectionExecutor<C,T>> handlers;
    
    public ServerConnectionExecutorsService(
            ServerConnectionFactory<Message.Server, T> connections,
            ParameterizedFactory<T, ServerConnectionExecutor<C,T>> factory) {
        this.connections = connections;
        this.factory = factory;
        this.handlers = new MapMaker().makeMap();
        
        connections.register(this);
    }
    
    public ServerConnectionFactory<Message.Server, T> connections() {
        return connections;
    }

    @Subscribe
    public void handleNewConnection(T connection) {
        new ConnectionListener(connection, factory.get(connection));
    }

    protected class ConnectionListener extends Pair<T, ServerConnectionExecutor<C,T>> {
        public ConnectionListener(T connection, ServerConnectionExecutor<C,T> handler) {
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

    @Override
    protected void startUp() throws Exception {
        connections.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        connections.stop().get();
    }

    @Override
    public Iterator<ServerConnectionExecutor<C,T>> iterator() {
        return handlers.values().iterator();
    }
}
