package edu.uw.zookeeper.net.intravm;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
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
import edu.uw.zookeeper.util.SettableFuturePromise;

@RunWith(JUnit4.class)
public class IntraVmTest {
    
    public static final InetAddress LOOPBACK;
    static {
        try {
            LOOPBACK = InetAddress.getByName(null);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
    
    public static <T> IdentityFactory<T> identityFactory() {
        return new IdentityFactory<T>();
    }
    
    public static class IdentityFactory<T> implements ParameterizedFactory<T, T> {
        @Override
        public T get(T value) {
            return value;
        }
    }

    public static Factory<ListeningExecutorService> sameThreadExecutors() {
        return Factories.holderOf(MoreExecutors.sameThreadExecutor());
    }
    
    public static Factory<EventBusPublisher> eventBusPublishers() {
        return new Factory<EventBusPublisher>() {
            @Override
            public EventBusPublisher get() {
                return EventBusPublisher.newInstance();
            }
        };
    }
    
    public static Factory<InetSocketAddress> loopbackAddresses(final int startPort) {
        final InetAddress host = LOOPBACK;
        
        return new Factory<InetSocketAddress>() {

            private final AtomicInteger nextPort = new AtomicInteger(startPort);
            
            @Override
            public InetSocketAddress get() {
                int port = nextPort.getAndIncrement();
                return new InetSocketAddress(host, port);
            }
        };
    }
    
    public static <T extends SocketAddress> Function<SocketAddress, IntraVmConnection<T>> connector(
            final IntraVmServerConnectionFactory<T,?,?> server) {
        return new Function<SocketAddress, IntraVmConnection<T>>() {
            @Override
            public IntraVmConnection<T> apply(SocketAddress input) {
                return server.connect();
            }
        };
    }
    
    public static class EndpointFactory<T extends SocketAddress> implements Factory<Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>>> {
        public static <T extends SocketAddress> EndpointFactory<T> newInstance(
                Factory<T> addresses,
                Factory<? extends Publisher> publishers, 
                Factory<? extends Executor> executors) {
            return new EndpointFactory<T>(addresses, publishers, executors);
        }
           
        protected final Factory<T> addresses;
        protected final Factory<? extends Publisher> publishers;
        protected final Factory<? extends Executor> executors;
        
        public EndpointFactory(
                Factory<T> addresses,
                Factory<? extends Publisher> publishers, 
                Factory<? extends Executor> executors) {
            this.addresses = addresses;
            this.publishers = publishers;
            this.executors = executors;
        }
        @Override
        public Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>> get() {
            return Pair.create(IntraVmConnectionEndpoint.create(addresses.get(), publishers.get(), executors.get()),
                    IntraVmConnectionEndpoint.create(addresses.get(), publishers.get(), executors.get()));
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
        testEndpoints(EndpointFactory.newInstance(loopbackAddresses(1), eventBusPublishers(), sameThreadExecutors()));
    }
    
    protected void testEndpoints(Factory<Pair<IntraVmConnectionEndpoint<InetSocketAddress>, IntraVmConnectionEndpoint<InetSocketAddress>>> endpointFactory) throws InterruptedException, ExecutionException {
        EventBusPublisher clientPublisher = EventBusPublisher.newInstance();
        EventBusPublisher serverPublisher = EventBusPublisher.newInstance();
        IdentityFactory<IntraVmConnection<InetSocketAddress>> connectionFactory = identityFactory();
        IntraVmServerConnectionFactory<InetSocketAddress, Object, IntraVmConnection<InetSocketAddress>> serverConnections = 
                IntraVmServerConnectionFactory.newInstance(
                        new InetSocketAddress(LOOPBACK, 0), clientPublisher, endpointFactory, connectionFactory);
        Function<SocketAddress, IntraVmConnection<InetSocketAddress>> connector = connector(serverConnections);
        IntraVmClientConnectionFactory<InetSocketAddress, Object, IntraVmConnection<InetSocketAddress>> clientConnections = 
                IntraVmClientConnectionFactory.newInstance(
                        connector, serverPublisher, connectionFactory);
        ConnectionFactory<?,?>[] connections = { clientConnections, serverConnections };
        for (ConnectionFactory<?,?> e: connections) {
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
        
        for (ConnectionFactory<?,?> e: connections) {
            e.stop().get();
            assertEquals(0, Iterables.size(e));
        }
    }
}
