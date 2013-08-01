package edu.uw.zookeeper.net.intravm;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.GetEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.EventBusPublisher;
import edu.uw.zookeeper.util.ParameterizedFactory;

@RunWith(JUnit4.class)
public class IntraVmTest {
    
    public static <T> IdentityFactory<T> identityFactory() {
        return new IdentityFactory<T>();
    }
    
    public static class IdentityFactory<T> implements ParameterizedFactory<T, T> {
        @Override
        public T get(T value) {
            return value;
        }
    }

    public static <T extends SocketAddress> Function<SocketAddress, IntraVmConnection<T>> connector(
            final IntraVmServerConnectionFactory<T,?> server) {
        return new Function<SocketAddress, IntraVmConnection<T>>() {
            @Override
            public IntraVmConnection<T> apply(SocketAddress input) {
                return server.connect();
            }
        };
    }
    
    public static <C extends Connection<?>> IntraVmServerConnectionFactory<InetSocketAddress, C> newServerFactory(
            int port,
            ParameterizedFactory<IntraVmConnection<InetSocketAddress>, C> connectionFactory) {
        return IntraVmServerConnectionFactory.newInstance(
                new InetSocketAddress(EndpointFactory.LOOPBACK, port), 
                EventBusPublisher.newInstance(), 
                EndpointFactory.defaults(), 
                connectionFactory);
    }
    
    public static <T extends SocketAddress, C extends Connection<?>> IntraVmClientConnectionFactory<T,C> newClientFactory(
            IntraVmServerConnectionFactory<T, ?> server,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        return IntraVmClientConnectionFactory.newInstance(
                connector(server), 
                EventBusPublisher.newInstance(), 
                connectionFactory);
    }
    
    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        IdentityFactory<IntraVmConnection<InetSocketAddress>> connectionFactory = identityFactory();
        IntraVmServerConnectionFactory<InetSocketAddress, IntraVmConnection<InetSocketAddress>> serverConnections = 
                newServerFactory(0, connectionFactory);
        IntraVmClientConnectionFactory<InetSocketAddress, IntraVmConnection<InetSocketAddress>> clientConnections = 
                newClientFactory(serverConnections, connectionFactory);
        ConnectionFactory<?>[] connections = { clientConnections, serverConnections };
        for (ConnectionFactory<?> e: connections) {
            e.start().get();
        }
        
        GetEvent<IntraVmConnection<InetSocketAddress>> clientConnectionEvent = GetEvent.newInstance(clientConnections);
        GetEvent<IntraVmConnection<InetSocketAddress>> serverConnectionEvent = GetEvent.newInstance(serverConnections);
        IntraVmConnection<InetSocketAddress> client = clientConnections.connect(serverConnections.listenAddress()).get();
        assertSame(client, Iterables.getOnlyElement(clientConnections));
        assertSame(client, clientConnectionEvent.get());
        IntraVmConnection<InetSocketAddress> server = Iterables.getOnlyElement(serverConnections);
        assertSame(server, serverConnectionEvent.get());
        
        String input = "hello";
        GetEvent<String> outputEvent = GetEvent.newInstance(server);
        ListenableFuture<String> inputFuture = client.write(input);
        server.read();
        assertEquals(input, outputEvent.get());
        inputFuture.get();
        
        assertEquals(Connection.State.CONNECTION_OPENED, client.state());
        assertEquals(Connection.State.CONNECTION_OPENED, server.state());
        
        GetEvent<Automaton.Transition<?>> clientClosingEvent = GetEvent.newInstance(client);
        GetEvent<Automaton.Transition<?>> serverClosingEvent = GetEvent.newInstance(server);
        client.close().get();
        server.close().get();
        
        assertEquals(Connection.State.CONNECTION_CLOSING, clientClosingEvent.get().to());
        assertEquals(Connection.State.CONNECTION_CLOSING, serverClosingEvent.get().to());

        assertEquals(Connection.State.CONNECTION_CLOSED, client.state());
        assertEquals(Connection.State.CONNECTION_CLOSED, server.state());
        
        for (ConnectionFactory<?> e: connections) {
            e.stop().get();
            assertEquals(0, Iterables.size(e));
        }
    }
}
