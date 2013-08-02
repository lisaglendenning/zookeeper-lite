package edu.uw.zookeeper.net.intravm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Publisher;

public class EndpointFactory<T extends SocketAddress> implements Factory<Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>>> {

    public static EndpointFactory<InetSocketAddress> defaults() {
        return create(loopbackAddresses(1), eventBusPublishers(), sameThreadExecutors());
    }
    
    public static <T extends SocketAddress> EndpointFactory<T> create(
            Factory<T> addresses,
            Factory<? extends Publisher> publishers, 
            Factory<? extends Executor> executors) {
        return new EndpointFactory<T>(addresses, publishers, executors);
    }

    public static final InetAddress LOOPBACK;
    static {
        try {
            LOOPBACK = InetAddress.getByName(null);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
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
}