package edu.uw.zookeeper.net.intravm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.ActorExecutor;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Publisher;

public class IntraVmEndpointFactory<V> implements Factory<IntraVmEndpoint<V>> {

    public static <V> IntraVmEndpointFactory<V> defaults() {
        return create(loopbackAddresses(1), eventBusPublishers(), sameThreadExecutors());
    }
    
    public static <V> IntraVmEndpointFactory<V> create(
            Factory<? extends SocketAddress> addresses,
            Factory<? extends Publisher> publishers, 
            Factory<? extends Executor> executors) {
        return new IntraVmEndpointFactory<V>(addresses, publishers, executors);
    }

    public static final InetAddress LOOPBACK;
    static {
        try {
            LOOPBACK = InetAddress.getByName(null);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    public static Factory<? extends Executor> sameThreadExecutors() {
        final Executor executor = MoreExecutors.sameThreadExecutor();
        return new Factory<Executor>() {
            @Override
            public Executor get() {
                return ActorExecutor.newInstance(executor);
            }
        };
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
    
    protected final Factory<? extends SocketAddress> addresses;
    protected final Factory<? extends Publisher> publishers;
    protected final Factory<? extends Executor> executors;
    
    public IntraVmEndpointFactory(
            Factory<? extends SocketAddress> addresses,
            Factory<? extends Publisher> publishers, 
            Factory<? extends Executor> executors) {
        this.addresses = addresses;
        this.publishers = publishers;
        this.executors = executors;
    }
    
    public Factory<? extends SocketAddress> addresses() {
        return addresses;
    }
    
    public Factory<? extends Publisher> publishers() {
        return publishers;
    }
    
    public Factory<? extends Executor> executors() {
        return executors;
    }
    
    @Override
    public IntraVmEndpoint<V> get() {
        return IntraVmEndpoint.<V>create(addresses.get(), publishers.get(), executors.get());
    }
}