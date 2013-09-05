package edu.uw.zookeeper.net.intravm;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.GetEvent;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;

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

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        IntraVmEndpointFactory<Object> endpoints = IntraVmEndpointFactory.defaults();
        IntraVmFactory factory = IntraVmFactory.newInstance(endpoints.addresses(), endpoints.publishers());
        IdentityFactory<IntraVmConnection<Object>> connectionFactory = identityFactory();
        IntraVmServerConnectionFactory<IntraVmConnection<Object>, Object> serverConnections = 
                factory.newServer(endpoints.addresses().get(), endpoints, connectionFactory);
        IntraVmClientConnectionFactory<IntraVmConnection<Object>, Object> clientConnections = 
                factory.newClient(endpoints, connectionFactory);
        ConnectionFactory<?>[] connections = { clientConnections, serverConnections };
        for (ConnectionFactory<?> e: connections) {
            e.startAsync();
            e.awaitRunning();
        }
        
        GetEvent<IntraVmConnection<Object>> clientConnectionEvent = GetEvent.create(clientConnections);
        GetEvent<IntraVmConnection<Object>> serverConnectionEvent = GetEvent.create(serverConnections);
        IntraVmConnection<Object> client = clientConnections.connect(serverConnections.listenAddress()).get();
        assertSame(client, Iterables.getOnlyElement(clientConnections));
        assertSame(client, clientConnectionEvent.get());
        IntraVmConnection<Object> server = Iterables.getOnlyElement(serverConnections);
        assertSame(server, serverConnectionEvent.get());
        
        String input = "hello";
        GetEvent<String> outputEvent = GetEvent.create(server);
        ListenableFuture<String> inputFuture = client.write(input);
        client.flush();
        server.read();
        assertEquals(input, outputEvent.get());
        assertTrue(input == inputFuture.get());
        
        assertEquals(Connection.State.CONNECTION_OPENED, client.state());
        assertEquals(Connection.State.CONNECTION_OPENED, server.state());
        
        GetEvent<Automaton.Transition<?>> clientClosingEvent = GetEvent.create(client);
        GetEvent<Automaton.Transition<?>> serverClosingEvent = GetEvent.create(server);
        client.close().get();
        server.close().get();
        
        assertEquals(Connection.State.CONNECTION_CLOSING, clientClosingEvent.get().to());
        assertEquals(Connection.State.CONNECTION_CLOSING, serverClosingEvent.get().to());

        assertEquals(Connection.State.CONNECTION_CLOSED, client.state());
        assertEquals(Connection.State.CONNECTION_CLOSED, server.state());
        
        for (ConnectionFactory<?> e: connections) {
            e.stopAsync();
            e.awaitTerminated();
            assertEquals(0, Iterables.size(e));
        }
    }
}
