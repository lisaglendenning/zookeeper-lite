package edu.uw.zookeeper.net.intravm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actors.ActorExecutor;
import edu.uw.zookeeper.common.Factory;

public abstract class AbstractIntraVmEndpointFactory<T extends AbstractIntraVmEndpoint<?,?,?,?>> implements Factory<T> {

    public static final InetAddress LOOPBACK;
    static {
        try {
            LOOPBACK = InetAddress.getByName(null);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
    
    public static Factory<? extends Executor> sameThreadExecutors() {
        return actorExecutors(MoreExecutors.directExecutor());
    }
    
    public static Factory<? extends Executor> actorExecutors(
            final Executor executor) {
        return new Factory<Executor>() {
            @Override
            public Executor get() {
                return ActorExecutor.newInstance(executor);
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
    
    protected final Supplier<? extends SocketAddress> addresses;
    protected final Supplier<? extends Executor> executors;
    
    public AbstractIntraVmEndpointFactory(
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Executor> executors) {
        this.addresses = addresses;
        this.executors = executors;
    }
    
    public Supplier<? extends SocketAddress> addresses() {
        return addresses;
    }
    
    public Supplier<? extends Executor> executors() {
        return executors;
    }
}