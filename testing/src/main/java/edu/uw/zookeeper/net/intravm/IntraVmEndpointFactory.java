package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.concurrent.Executor;
import com.google.common.base.Supplier;

public class IntraVmEndpointFactory<I,O> extends AbstractIntraVmEndpointFactory<IntraVmEndpoint<I,O>> {

    public static <I,O> IntraVmEndpointFactory<I,O> defaults() {
        return create(loopbackAddresses(1), sameThreadExecutors());
    }
    
    public static <I,O> IntraVmEndpointFactory<I,O> create(
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Executor> executors) {
        return new IntraVmEndpointFactory<I,O>(addresses, executors);
    }

    public IntraVmEndpointFactory(
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Executor> executors) {
        super(addresses, executors);
    }

    @Override
    public IntraVmEndpoint<I,O> get() {
        return IntraVmEndpoint.<I,O>newInstance(addresses.get(), executors.get());
    }
}