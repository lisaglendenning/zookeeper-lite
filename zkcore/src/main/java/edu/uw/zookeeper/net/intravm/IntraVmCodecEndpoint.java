package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.LoggingMarker;

public class IntraVmCodecEndpoint<I,O,T extends Codec<I,? extends O,? extends I,?>> extends AbstractIntraVmEndpoint<I,O,ByteBuf,ByteBuf> {

    public static <I,O,T extends Codec<I,? extends O,? extends I,?>> IntraVmCodecEndpoint<I,O,T> newInstance(
            ByteBufAllocator allocator,
            T codec,
            SocketAddress address,
            Executor executor) {
        Logger logger = LogManager.getLogger(IntraVmCodecEndpoint.class);
        return new IntraVmCodecEndpoint<I,O,T>(
                allocator, 
                codec, 
                address, 
                executor,
                logger,  
                IntraVmPublisher.<O>defaults(executor, logger));
    }
    
    protected final ByteBufAllocator allocator;
    protected final T codec;
    
    protected IntraVmCodecEndpoint(
            ByteBufAllocator allocator,
            T codec,
            SocketAddress address,
            Executor executor,
            Logger logger,
            IntraVmPublisher<O> publisher) {
        super(address, executor, logger, publisher);
        this.allocator = allocator;
        this.codec = codec;
    }
    
    public T getCodec() {
        return codec;
    }
    
    @Override
    public <U extends I> ListenableFuture<U> write(U message, AbstractIntraVmEndpoint<?,?,?,? super ByteBuf> remote) {
        if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
            CallablePromiseTask<EncodingEndpointWrite<U>,U> task = CallablePromiseTask.create(new EncodingEndpointWrite<U>(remote, message), SettableFuturePromise.<U>create());
            LoggingFutureListener.listen(logger, task);
            execute(task);
            return task;
        } else {
            return Futures.immediateFailedFuture(new ClosedChannelException());
        }
    }

    @Override
    public boolean read(ByteBuf message) {
        if (state().compareTo(Connection.State.CONNECTION_CLOSING) < 0) {
            execute(new DecodingEndpointRead(message));
            return true;
        }
        return false;
    }
    
    public class DecodingEndpointRead extends AbstractEndpointRead {

        public DecodingEndpointRead(ByteBuf message) {
            super(message);
        }
        
        @Override
        protected void doRead() {
            Optional<? extends O> output;
            try { 
                output = codec.decode(get());
            } catch (IOException e) {
                logger.warn(LoggingMarker.NET_MARKER.get(), "{}", get(), e);
                close();
                return;
            }
            if (output.isPresent()) {
                O message = output.get();
                logger.trace(LoggingMarker.NET_MARKER.get(), "DECODED {} ({})", message, this);
                publisher.send(message);
            }
        }
    }
    
    public class EncodingEndpointWrite<U extends I> extends AbstractEndpointWrite<U> {

        public EncodingEndpointWrite(
                AbstractIntraVmEndpoint<?,?,?,? super ByteBuf> remote,
                U task) {
            super(remote,task);
        }
        
        @Override
        public Optional<U> call() throws IOException {
            if (state() != Connection.State.CONNECTION_CLOSED) {
                ByteBuf output = allocator.buffer();
                logger.trace(LoggingMarker.NET_MARKER.get(), "ENCODING {}", this);
                try {
                    codec.encode(message, output);
                } catch (IOException e) {
                    logger.warn(LoggingMarker.NET_MARKER.get(), "{}", this, e);
                    throw e;
                }
                if (remote.read(output)) {
                    return Optional.of(message);
                } else {
                    close();
                    throw new ClosedChannelException();
                }
            } else {
                throw new ClosedChannelException();
            }
        }
    }
}