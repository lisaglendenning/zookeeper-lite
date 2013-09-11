package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;

import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;

import edu.uw.zookeeper.common.AbstractPair;


public final class Frame extends AbstractPair<IntHeader, ByteBuf> implements Encodable, ReferenceCounted {
    
    public static Frame fromBuffer(ByteBuf buffer) {
        int length = buffer.readableBytes();
        return of(IntHeader.of(length), buffer);
    }

    public static Frame of(IntHeader header, ByteBuf buffer) {
        return new Frame(header, buffer);
    }
    
    public static final class FramedEncoder<T> implements Encoder<T> {

        public static <T> FramedEncoder<T> create(Encoder<T> messageEncoder) {
            return new FramedEncoder<T>(messageEncoder);
        }

        private final Logger logger = LogManager
                .getLogger(getClass());
        private final Encoder<T> messageEncoder;
        
        public FramedEncoder(Encoder<T> messageEncoder) {
            this.messageEncoder = checkNotNull(messageEncoder);
        }
        
        public Encoder<T> messageEncoder() {
            return messageEncoder();
        }

        @Override
        public void encode(T input, ByteBuf output) throws IOException {
            int beginIndex = output.writerIndex();
            try {
                int headerLength = IntHeader.length();
                if (output.writableBytes() >= headerLength) {
                    output.writerIndex(beginIndex + headerLength);
                } else {
                    output.writeInt(0);
                }
                messageEncoder.encode(input, output);
                int endIndex = output.writerIndex();
                int length = endIndex - beginIndex - headerLength;
                checkState(length >= 0);
                output.writerIndex(beginIndex);
                output.writeInt(length);
                output.writerIndex(endIndex);
            } catch (Exception e) {
                logger.warn("Encoding {}", input, e);
                output.writerIndex(beginIndex);
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Shareable
     */
    public static final class FrameDecoder implements Decoder<Optional<Frame>> {

        public static FrameDecoder getDefault() {
            return create(Range.atLeast(Integer.valueOf(0)));
        }
        
        public static FrameDecoder create(Range<Integer> bounds) {
            return new FrameDecoder(bounds);
        }
        
        private final Range<Integer> bounds;
        
        public FrameDecoder(Range<Integer> bounds) {
            this.bounds = checkNotNull(bounds);
        }
        
        public Range<Integer> bounds() {
            return bounds;
        }
    
        /**
         * DO NOT discard read bytes until you are done with the returned Frame.
         * Make sure to release() the returned Frame when you are done with it.
         */
        @Override
        public Optional<Frame> decode(ByteBuf input) {
            Optional<Frame> output = Optional.absent();
            input.markReaderIndex();
            try {
                Optional<IntHeader> headerOutput = IntHeader.decode(input);
                if (headerOutput.isPresent()) {
                    IntHeader header = headerOutput.get();
                    int length = header.intValue();
                    if (! bounds.contains(length)) {
                        throw new IllegalArgumentException(String.format("Out of bounds frame header %s (%s)", header, bounds));
                    }
                    if (input.readableBytes() >= length) {
                        ByteBuf buffer = (length > 0) 
                                ? input.readSlice(length)
                                : Unpooled.EMPTY_BUFFER;
                        input.retain();
                        Frame frame = Frame.of(header, buffer);
                        output = Optional.of(frame);
                    }
                }
            } finally {
                if (! output.isPresent()) {
                    input.resetReaderIndex();
                }
            }
            return output;
        }
    }
    
    public static final class FramedDecoder<T> implements Decoder<Optional<T>> {
        
        public static <T> FramedDecoder<T> create(
                FrameDecoder frameDecoder,
                Decoder<T> messageDecoder) {
            return new FramedDecoder<T>(frameDecoder, messageDecoder);
        }
        
        private final Logger logger = LogManager
                .getLogger(getClass());
        private final FrameDecoder frameDecoder;
        private final Decoder<T> messageDecoder;
        
        public FramedDecoder(
                FrameDecoder frameDecoder,
                Decoder<T> messageDecoder) {
            this.frameDecoder = checkNotNull(frameDecoder);
            this.messageDecoder = checkNotNull(messageDecoder);
        }
        
        public FrameDecoder frameDecoder() {
            return frameDecoder;
        }
        
        public Decoder<T> messageDecoder() {
            return messageDecoder;
        }
    
        @Override
        public Optional<T> decode(ByteBuf input) throws IOException {
            Optional<T> output = Optional.absent(); 
            Optional<Frame> frameOutput = frameDecoder.decode(input);
            if (frameOutput.isPresent()) {
                Frame frame = frameOutput.get();
                try {
                    output = Optional.of(messageDecoder.decode(frame.buffer()));
                    // it's probably an error if we didn't consume the entire frame
                    if (frame.buffer().readableBytes() > 0) {
                        logger.warn("Skipping {} unread bytes after {}",
                                frame.buffer().readableBytes(), output.get());
                        frame.buffer().skipBytes(frame.buffer().readableBytes());
                    }
                } catch (Exception e) {
                    logger.warn("Decoding {}", frame, e);
                    Throwables.propagateIfInstanceOf(e, IOException.class);
                    throw Throwables.propagate(e);
                } finally {
                    frame.release();
                }
            }
            return output;
        }
    }

    public Frame(IntHeader header, ByteBuf buffer) {
        super(checkNotNull(header), checkNotNull(buffer));
        checkState(header.intValue() == buffer.readableBytes());
    }
    
    public IntHeader header() {
        return first;
    }
    
    public ByteBuf buffer() {
        return second;
    }

    @Override
    public void encode(ByteBuf output) throws IOException {
        first.encode(output);
        output.writeBytes(second);
    }

    @Override
    public int refCnt() {
        return second.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        return second.retain();
    }

    @Override
    public ReferenceCounted retain(int increment) {
        return second.retain(increment);
    }

    @Override
    public boolean release() {
        return second.release();
    }

    @Override
    public boolean release(int decrement) {
        return second.release(decrement);
    }
}
