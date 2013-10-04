package edu.uw.zookeeper.net.intravm;

import static com.google.common.base.Preconditions.checkState;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.Logging;
import edu.uw.zookeeper.protocol.Codec;

public class IntraVmCodecEndpoint<I, T extends Codec<? super I, ? extends Optional<?>>> extends IntraVmEndpoint<ByteBuf> {

    public static <I, T extends Codec<? super I, ? extends Optional<?>>> IntraVmCodecEndpoint<I,T> create(
            ByteBufAllocator allocator,
            Pair<Class<I>, T> codec,
            SocketAddress address,
            Publisher publisher,
            Executor executor) {
        return IntraVmCodecEndpoint.<I,T>builder(
                allocator,
                address, 
                publisher,
                executor).setCodec(codec).build();
    }

    public static <I, T extends Codec<? super I, ? extends Optional<?>>> Builder<I,T> builder(
            ByteBufAllocator allocator,
            SocketAddress address,
            Publisher publisher,
            Executor executor) {
        return new Builder<I,T>(allocator, address, publisher, executor);
    }
    
    public static class Builder<I, T extends Codec<? super I, ? extends Optional<?>>> extends IntraVmEndpoint.Builder<ByteBuf> {

        protected final ByteBufAllocator allocator;
        protected Pair<Class<I>, ? extends T> codec;
        
        public Builder(
                ByteBufAllocator allocator,
                SocketAddress address,
                Publisher publisher,
                Executor executor) {
            this(allocator, address, publisher, executor, LogManager.getLogger(IntraVmCodecEndpoint.class));
        }
        
        public Builder(
                ByteBufAllocator allocator,
                SocketAddress address,
                Publisher publisher,
                Executor executor,
                Logger logger) {
            super(address, publisher, executor, logger);
            this.allocator = allocator;
            this.codec = null;
        }
        
        public Builder<I,T> setCodec(Pair<Class<I>, ? extends T> codec) {
            this.codec = codec;
            return this;
        }
        
        @Override
        public IntraVmCodecEndpoint<I,T> build() {
            checkState(codec != null);
            
            return new IntraVmCodecEndpoint<I,T>(
                    allocator, codec, address, logger, executor, publisher, mailbox, stopped);
        }
    }
    
    protected final ByteBufAllocator allocator;
    protected final Class<I> type;
    protected final T codec;
    
    protected IntraVmCodecEndpoint(
            ByteBufAllocator allocator,
            Pair<Class<I>, ? extends T> codec,
            SocketAddress address,
            Logger logger,
            Executor executor,
            IntraVmPublisher publisher,
            Queue<Optional<? extends ByteBuf>> mailbox,
            Promise<IntraVmEndpoint<ByteBuf>> stopped) {
        super(address, logger, executor, publisher, mailbox, stopped);
        this.allocator = allocator;
        this.type = codec.first();
        this.codec = codec.second();
    }
    
    public Pair<Class<I>, T> getCodec() {
        return Pair.create(type, codec);
    }
    
    @Override
    public <U> ListenableFuture<U> write(Optional<U> message, IntraVmEndpoint<?> remote) {
        @SuppressWarnings("unchecked")
        EncodingSendTask<U> task = new EncodingSendTask<U>((IntraVmEndpoint<? super ByteBuf>) remote, message, LoggingPromise.create(logger, SettableFuturePromise.<U>create()));
        execute(task);
        return task;
    }

    @Override
    protected void doPost(ByteBuf input) {
        Optional<?> output;
        try { 
            output = codec.decode(input);
        } catch (IOException e) {
            logger.warn(Logging.NET_MARKER, "{}", input, e);
            stop();
            return;
        }
        if (output.isPresent()) {
            publisher.post(output.get());
        }
    }

    protected class EncodingSendTask<U> extends PromiseTask<Optional<U>, U> implements Runnable {

        protected final IntraVmEndpoint<? super ByteBuf> remote;
        
        public EncodingSendTask(
                IntraVmEndpoint<? super ByteBuf> remote,
                Optional<U> task, 
                Promise<U> promise) {
            super(task, promise);
            this.remote = remote;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public synchronized void run() {
            if(!isDone()) {
                ByteBuf output;
                if (task().isPresent()) {
                    output = allocator.buffer();
                    try {
                        codec.encode((I) task().get(), output);
                    } catch (IOException e) {
                        logger.warn(Logging.NET_MARKER, "{}", e);
                        setException(e);
                        return;
                    }
                } else {
                    output = null;
                }
                if (remote.send(Optional.fromNullable(output))) {
                    set(task().orNull());
                } else {
                    setException(new IllegalStateException(Connection.State.CONNECTION_CLOSING.toString()));
                }
            }
        }
    }
}