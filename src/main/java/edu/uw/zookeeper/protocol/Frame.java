package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Range;

import edu.uw.zookeeper.util.AbstractPair;


public class Frame extends AbstractPair<IntHeader, ByteBuf> implements Encodable {

    public static Frame fromEncodable(Encodable input, ByteBufAllocator output) throws IOException {
        ByteBuf buffer = input.encode(output);
        return fromBuffer(buffer);
    }
    
    public static Frame fromBuffer(ByteBuf buffer) {
        int length = buffer.readableBytes();
        return of(IntHeader.of(length), buffer);
    }

    public static Frame of(IntHeader header, ByteBuf buffer) {
        return new Frame(header, buffer);
    }
    
    public static class FramedEncoder<T> implements Encoder<T> {

        public static <T> FramedEncoder<T> create(Encoder<T> messageEncoder) {
            return new FramedEncoder<T>(messageEncoder);
        }
        
        protected final Encoder<T> messageEncoder;
        
        public FramedEncoder(Encoder<T> messageEncoder) {
            this.messageEncoder = messageEncoder;
        }

        @Override
        public ByteBuf encode(T input, ByteBufAllocator output) throws IOException {
            ByteBuf encoded = messageEncoder.encode(input, output);
            Frame frame = Frame.fromBuffer(encoded);
            return frame.encode(output);
        }
    }

    public static class FrameDecoder implements Decoder<Optional<Frame>> {

        public static FrameDecoder getDefault() {
            return create(Range.atLeast(Integer.valueOf(0)));
        }
        
        public static FrameDecoder create(Range<Integer> bounds) {
            return new FrameDecoder(bounds);
        }
        
        protected final Range<Integer> bounds;
        
        protected FrameDecoder(Range<Integer> bounds) {
            this.bounds = checkNotNull(bounds);
        }
        
        public Range<Integer> bounds() {
            return bounds;
        }
    
        @Override
        public Optional<Frame> decode(ByteBuf input) throws IOException {
            Optional<Frame> output = Optional.absent();
            input.markReaderIndex();
            try {
                Optional<IntHeader> header = IntHeader.decode(input);
                if (header.isPresent()) {
                    int length = header.get().intValue();
                    if (! bounds.contains(length)) {
                        throw new IllegalArgumentException(String.format("Invalid frame header 0x%x", length));
                    }
                    if (input.readableBytes() >= length) {
                        ByteBuf buffer = (length > 0) 
                                ? input.slice(input.readerIndex(), length)
                                : Unpooled.EMPTY_BUFFER;
                        Frame frame = Frame.of(header.get(), buffer);
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
    
    public static class FramedDecoder<T> implements Decoder<Optional<T>> {
        
        public static <T> FramedDecoder<T> create(
                FrameDecoder frameDecoder,
                Decoder<T> messageDecoder) {
            return new FramedDecoder<T>(frameDecoder, messageDecoder);
        }
        
        private final Logger logger = LoggerFactory
                .getLogger(FramedDecoder.class);
        private final FrameDecoder frameDecoder;
        private final Decoder<T> messageDecoder;
        
        private FramedDecoder(
                FrameDecoder frameDecoder,
                Decoder<T> messageDecoder) {
            this.frameDecoder = checkNotNull(frameDecoder);
            this.messageDecoder = checkNotNull(messageDecoder);
        }
    
        @Override
        public Optional<T> decode(ByteBuf input) throws IOException {
            Optional<T> output = Optional.absent(); 
            Optional<Frame> optFrame = frameDecoder.decode(input);
            if (optFrame.isPresent()) {
                Frame frame = optFrame.get();
                ByteBuf frameBuffer = frame.buffer();
                int readable = frameBuffer.readableBytes();
                checkArgument(readable < frame.length());
                
                output = Optional.of(messageDecoder.decode(frameBuffer));
                
                // make sure we consume the entire frame
                readable = frame.length() - (readable - frameBuffer.readableBytes());
                if (readable > 0) {
                    logger.debug("Skipping {} unread bytes after {}",
                            readable, output);
                    frameBuffer.skipBytes(readable);
                }
            }
            return output;
        }
    }

    public Frame(IntHeader header, ByteBuf buffer) {
        super(checkNotNull(header), checkNotNull(buffer));
    }
    
    public IntHeader header() {
        return first;
    }
    
    public ByteBuf buffer() {
        return second;
    }
    
    public int length() {
        return header().intValue();
    }

    @Override
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        checkArgument(header().intValue() == buffer().readableBytes());
        return Buffers.composite(output, header().encode(output), buffer());
    }
}
