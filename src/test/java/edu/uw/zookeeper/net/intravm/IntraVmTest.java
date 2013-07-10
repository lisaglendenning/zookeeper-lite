package edu.uw.zookeeper.net.intravm;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.EventBusPublisher;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableFuturePromise;

@RunWith(JUnit4.class)
public class IntraVmTest {

    public static class IdentityFactory<T> implements ParameterizedFactory<T, T> {
        @Override
        public T get(T value) {
            return value;
        }
    }
    
    public static class EndpointFactory implements Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> {
        protected final Factory<? extends Publisher> publishers;
        protected final Factory<? extends Executor> executors;
        
        public EndpointFactory(
                Factory<? extends Publisher> publishers, 
                Factory<? extends Executor> executors) {
            this.publishers = publishers;
            this.executors = executors;
        }
        @Override
        public Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint> get() {
            return Pair.create(IntraVmConnectionEndpoint.create(publishers.get(), executors.get()),
                    IntraVmConnectionEndpoint.create(publishers.get(), executors.get()));
        }
    };
    
    public static class GetEvent<T> extends ForwardingPromise<T> {

        public static <T> GetEvent<T> newInstance(
                Eventful eventful) {
            Promise<T> delegate = SettableFuturePromise.create();
            return newInstance(eventful, delegate);
        }
        
        public static <T> GetEvent<T> newInstance(
                Eventful eventful,
                Promise<T> delegate) {
            return new GetEvent<T>(eventful, delegate);
        }
        
        protected final Promise<T> delegate;
        protected final Eventful eventful;
        
        public GetEvent(
                Eventful eventful,
                Promise<T> delegate) {
            this.eventful = eventful;
            this.delegate = delegate;
            eventful.register(this);
        }
        
        @Subscribe
        public void handleEvent(T event) {
            eventful.unregister(this);
            set(event);
        }

        @Override
        protected Promise<T> delegate() {
            return delegate;
        }
    }
    
    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Reference<? extends Executor> executors = Factories.holderOf(MoreExecutors.sameThreadExecutor());
        Factory<? extends Publisher> publishers = new Factory<EventBusPublisher>() {
            @Override
            public EventBusPublisher get() {
                return EventBusPublisher.newInstance();
            }
        };
        testEndpoints(new EndpointFactory(publishers, executors));
    }
    
    protected void testEndpoints(Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory) throws InterruptedException, ExecutionException {
        EventBusPublisher clientPublisher = EventBusPublisher.newInstance();
        EventBusPublisher serverPublisher = EventBusPublisher.newInstance();
        IdentityFactory<IntraVmConnection> connectionFactory = new IdentityFactory<IntraVmConnection>();
        IntraVmServerConnectionFactory<Object, IntraVmConnection> serverConnections = 
                IntraVmServerConnectionFactory.newInstance(
                        clientPublisher, endpointFactory, connectionFactory);
        IntraVmClientConnectionFactory<Object, IntraVmConnection> clientConnections = 
                IntraVmClientConnectionFactory.newInstance(
                        serverPublisher, connectionFactory);
        ConnectionFactory<?,?>[] connections = { clientConnections, serverConnections };
        for (ConnectionFactory<?,?> e: connections) {
            e.start().get();
        }
        
        GetEvent<IntraVmConnection> clientConnectionEvent = GetEvent.newInstance(clientConnections);
        GetEvent<IntraVmConnection> serverConnectionEvent = GetEvent.newInstance(serverConnections);
        IntraVmConnection client = clientConnections.connect(serverConnections.listenAddress()).get();
        assertSame(client, Iterables.getOnlyElement(clientConnections));
        assertSame(client, clientConnectionEvent.get());
        IntraVmConnection server = Iterables.getOnlyElement(serverConnections);
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
        
        for (ConnectionFactory<?,?> e: connections) {
            e.stop().get();
            assertEquals(0, Iterables.size(e));
        }
    }
}
