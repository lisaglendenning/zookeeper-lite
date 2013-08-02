package edu.uw.zookeeper.protocol.server;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;

public class ServerConnectionExecutorsService<C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> extends AbstractIdleService implements Iterable<ServerConnectionExecutor<C,T>> {

    public static <C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> ServerConnectionExecutorsService<C,T> newInstance(
            ServerConnectionFactory<T> connections,
            ServerTaskExecutor server) {
        return new ServerConnectionExecutorsService<C,T>(
                connections, 
                ServerConnectionExecutor.<C,T>factory(
                        server.getAnonymousExecutor(), 
                        server.getConnectExecutor(), 
                        server.getSessionExecutor()));
    }

    protected final ServerConnectionFactory<T> connections;
    protected final ParameterizedFactory<T, ServerConnectionExecutor<C,T>> factory;
    protected final ConcurrentMap<T, ServerConnectionExecutor<C,T>> handlers;
    
    public ServerConnectionExecutorsService(
            ServerConnectionFactory<T> connections,
            ParameterizedFactory<T, ServerConnectionExecutor<C,T>> factory) {
        this.connections = connections;
        this.factory = factory;
        this.handlers = new MapMaker().makeMap();
    }
    
    @Override
    public Iterator<ServerConnectionExecutor<C,T>> iterator() {
        return handlers.values().iterator();
    }

    public ServerConnectionFactory<T> connections() {
        return connections;
    }

    @Subscribe
    public void handleNewConnection(T connection) {
        new ConnectionListener(connection, factory.get(connection));
    }

    @Override
    protected void startUp() throws InterruptedException, ExecutionException {
        connections.register(this);
        connections.start().get();
    }

    @Override
    protected void shutDown() throws InterruptedException, ExecutionException {
        connections.stop().get();
        connections.unregister(this);
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
}
