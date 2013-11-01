package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.google.common.base.Supplier;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.net.Codec;

public class IntraVmCodecEndpointFactory<I,O,T extends Codec<I,? extends O,? extends I,?>> extends AbstractIntraVmEndpointFactory<IntraVmCodecEndpoint<I,O,T>> {

    public static <I,O,T extends Codec<I,? extends O,? extends I,?>> IntraVmCodecEndpointFactory<I,O,T> defaults(
            Supplier<? extends T> codecs,
            Supplier<? extends SocketAddress> addresses) {
        return create(codecs, unpooled(), addresses, sameThreadExecutors());
    }
    
    public static <I,O,T extends Codec<I,? extends O,? extends I,?>> IntraVmCodecEndpointFactory<I,O,T> create(
            Supplier<? extends T> codecs,
            Supplier<? extends ByteBufAllocator> allocators,
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Executor> executors) {
        return new IntraVmCodecEndpointFactory<I,O,T>(
                codecs, allocators, addresses, executors);
    }

    public static Factory<ByteBufAllocator> unpooled() {
        return new Factory<ByteBufAllocator>() {
            @Override
            public ByteBufAllocator get() {
                return new UnpooledByteBufAllocator(false);
            }
        };
    }

    protected final Supplier<? extends ByteBufAllocator> allocators;
    protected final Supplier<? extends T> codecs;

    public IntraVmCodecEndpointFactory(
            Supplier<? extends T> codecs,
            Supplier<? extends ByteBufAllocator> allocators,
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Executor> executors) {
        super(addresses, executors);
        this.allocators = allocators;
        this.codecs = codecs;
    }
    
    @Override
    public IntraVmCodecEndpoint<I,O,T> get() {
        return IntraVmCodecEndpoint.<I,O,T>newInstance(allocators.get(), codecs.get(), addresses.get(), executors.get());
    }
}