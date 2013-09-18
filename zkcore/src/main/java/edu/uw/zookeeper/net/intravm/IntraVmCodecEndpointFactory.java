package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.Codec;

public class IntraVmCodecEndpointFactory<I, T extends Codec<? super I, ? extends Optional<?>>> extends IntraVmEndpointFactory<ByteBuf> {

    public static <I, T extends Codec<? super I, ? extends Optional<?>>> IntraVmCodecEndpointFactory<I,T> defaults(
            Supplier<? extends SocketAddress> addresses,
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecs) {
        return create(codecs, unpooled(), addresses, EventBusPublisher.factory(), sameThreadExecutors());
    }
    
    public static <I, T extends Codec<? super I, ? extends Optional<?>>> IntraVmCodecEndpointFactory<I,T> create(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecs,
            Supplier<? extends ByteBufAllocator> allocators,
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Publisher> publishers, 
            Supplier<? extends Executor> executors) {
        return new IntraVmCodecEndpointFactory<I,T>(
                codecs, allocators, addresses, publishers, executors);
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
    protected final ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecs;

    public IntraVmCodecEndpointFactory(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecs,
            Supplier<? extends ByteBufAllocator> allocators,
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Publisher> publishers, 
            Supplier<? extends Executor> executors) {
        super(addresses, publishers, executors);
        this.allocators = allocators;
        this.codecs = codecs;
    }
    
    @Override
    public IntraVmCodecEndpoint<I,T> get() {
        IntraVmCodecEndpoint.Builder<I,T> builder = IntraVmCodecEndpoint.<I,T>builder(allocators.get(), addresses.get(), publishers.get(), executors.get());
        return builder.setCodec(codecs.get(builder.getPublisher())).build();
    }
}