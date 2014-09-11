package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.LoggingMarker;

public final class IntraVmCodecEndpoint<I,O,T extends Codec<I,? extends O,? extends I,?>> extends AbstractIntraVmEndpoint<I,O,ByteBuf,ByteBuf> {

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
    private final DecodingEndpointReader reader;
    private final DecodingEndpointWriter writer;
    
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
        this.reader = new DecodingEndpointReader();
        this.writer = new DecodingEndpointWriter();
    }
    
    public T codec() {
        return codec;
    }

    @Override
    public DecodingEndpointReader reader() {
        return reader;
    }

    @Override
    public DecodingEndpointWriter writer() {
        return writer;
    }

    public final class DecodingEndpointReader extends AbstractEndpointReader {

        protected DecodingEndpointReader() {
            super(Queues.<ByteBuf>newLinkedBlockingDeque());
        }

        @Override
        protected boolean apply(ByteBuf input) throws Exception {
            Optional<? extends O> output;
            try { 
                output = codec.decode(input);
            } catch (IOException e) {
                logger.warn(LoggingMarker.NET_MARKER.get(), "{} ({})", input, this, e);
                throw e;
            }
            if (output.isPresent()) {
                O message = output.get();
                logger.trace(LoggingMarker.NET_MARKER.get(), "DECODED {} ({})", message, this);
                return publisher.send(message);
            } else {
                ByteBuf next = mailbox().poll();
                if (next != null) {
                    CompositeByteBuf composite;
                    if (input instanceof CompositeByteBuf) {
                        composite = (CompositeByteBuf) input;
                    } else {
                        composite = Unpooled.compositeBuffer();
                        composite.addComponent(input);
                    }
                    composite.addComponent(next);
                    input = composite;
                }
                mailbox().addFirst(input);
                if (state() == State.TERMINATED) {
                    mailbox().poll();
                    return false;
                }
                return (next != null);
            }
        }
    }
    
    public final class DecodingEndpointWriter extends AbstractEndpointWriter {

        protected DecodingEndpointWriter() {
            super(Queues.<EndpointWrite<? extends I,ByteBuf>>newConcurrentLinkedQueue());
        }

        @Override
        protected boolean apply(EndpointWrite<? extends I,ByteBuf> input) throws Exception {
            ByteBuf output = allocator.buffer();
            logger.trace(LoggingMarker.NET_MARKER.get(), "ENCODING {} ({})", input, this);
            try {
                codec.encode(input.message(), output);
            } catch (IOException e) {
                logger.warn(LoggingMarker.NET_MARKER.get(), "{} ({})", input, this, e);
                throw e;
            }
            if (input.remote().reader().send(output)) {
                input.run();
                return true;
            } else {
                Exception e = new ClosedChannelException();
                input.setException(e);
                throw e;
            }
        }
    }
}