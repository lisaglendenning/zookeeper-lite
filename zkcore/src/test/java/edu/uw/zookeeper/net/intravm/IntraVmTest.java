package edu.uw.zookeeper.net.intravm;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.net.GetConnection;
import edu.uw.zookeeper.net.GetConnectionRead;
import edu.uw.zookeeper.net.GetConnectionState;
import edu.uw.zookeeper.net.ServerConnectionFactory;

@RunWith(JUnit4.class)
public class IntraVmTest {
    
    protected static final Logger logger = LogManager.getLogger(IntraVmTest.class);
    
    public static class LoggingConnectionListener implements Connection.Listener<Object> {

        protected final Logger logger;
        
        public LoggingConnectionListener(Logger logger) {
            this.logger = logger;
        }
        
        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            logger.trace(state);
        }

        @Override
        public void handleConnectionRead(Object message) {
            logger.trace(message);
        }
    }
    
    @Test(timeout=10000)
    public void test() throws InterruptedException, ExecutionException {
        IntraVmEndpointFactory<String,String> endpoints = IntraVmEndpointFactory.defaults();
        IntraVmFactory<String,String> factory = IntraVmFactory.newInstance(endpoints.addresses());
        ParameterizedFactory<Pair<? extends IntraVmEndpoint<String,String>, ? extends AbstractIntraVmEndpoint<?,?,?,? super String>>, ? extends IntraVmConnection<String,String>> connectionFactory = IntraVmConnection.factory();
        ServerConnectionFactory<? extends IntraVmConnection<String,String>> serverConnections = 
                factory.newServer(endpoints.addresses().get(), endpoints, 
                        connectionFactory);
        ClientConnectionFactory<? extends IntraVmConnection<String,String>> clientConnections = 
                factory.newClient(endpoints, connectionFactory);
        ConnectionFactory<?>[] connections = { clientConnections, serverConnections };
        for (ConnectionFactory<?> e: connections) {
            e.startAsync();
            e.awaitRunning();
        }
        
        GetConnection<? extends IntraVmConnection<String,String>> clientConnectionEvent = GetConnection.create(clientConnections);
        GetConnection<? extends IntraVmConnection<String,String>> serverConnectionEvent = GetConnection.create(serverConnections);
        IntraVmConnection<String,String> client = clientConnections.connect(serverConnections.listenAddress()).get();
        assertSame(client, Iterables.getOnlyElement(clientConnections));
        assertSame(client, clientConnectionEvent.get());
        IntraVmConnection<String,String> server = Iterables.getOnlyElement(serverConnections);
        assertSame(server, serverConnectionEvent.get());
        
        LoggingConnectionListener serverListener = new LoggingConnectionListener(logger);
        server.subscribe(serverListener);
        LoggingConnectionListener clientListener = new LoggingConnectionListener(logger);
        client.subscribe(clientListener);
        
        String input = "hello";
        GetConnectionRead<String> outputEvent = GetConnectionRead.create(server);
        ListenableFuture<String> inputFuture = client.write(input);
        client.flush();
        server.read();
        assertEquals(input, outputEvent.get());
        assertEquals(input, inputFuture.get());
        
        assertTrue(client.state().compareTo(Connection.State.CONNECTION_OPENED) <= 0);
        assertEquals(Connection.State.CONNECTION_OPENED, server.state());
        
        GetConnectionState clientEvent = GetConnectionState.create(client);
        GetConnectionState serverEvent = GetConnectionState.create(server);
        
        client.close().get();
        server.close().get();
        
        assertEquals(Connection.State.CONNECTION_CLOSING, clientEvent.get().to());
        assertEquals(Connection.State.CONNECTION_CLOSING, serverEvent.get().to());

        assertEquals(Connection.State.CONNECTION_CLOSED, client.state());
        assertEquals(Connection.State.CONNECTION_CLOSED, server.state());
        
        for (ConnectionFactory<?> e: connections) {
            e.stopAsync();
            e.awaitTerminated();
            assertEquals(0, Iterables.size(e));
        }
    }
}
