package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import org.apache.jute.BinaryInputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import edu.uw.zookeeper.data.Buffers;
import edu.uw.zookeeper.data.Decoder;
import edu.uw.zookeeper.data.Encodable;
import edu.uw.zookeeper.data.Encoder;
import edu.uw.zookeeper.util.Factory;


public class Frame implements Encodable {

    public static Frame fromEncodable(Encodable input, ByteBufAllocator output) throws IOException {
        ByteBuf buffer = input.encode(output);
        return create(buffer);
    }
    
    public static Frame create(ByteBuf buffer) {
        int length = buffer.readableBytes();
        return create(Header.create(length), buffer);
    }

    public static Frame create(Header header, ByteBuf buffer) {
        int length = header.intValue();
        checkArgument(validLength(length));
        return new Frame(header, buffer);
    }
    
    /**
     * Stateless
     */
    public static class FrameEncoder implements Encoder<Encodable> {
        
        public static enum Singleton implements Factory<Encoder<Encodable>> {
            INSTANCE;
            
            public static Encoder<Encodable> getInstance() {
                return INSTANCE.get();
            }
            
            private final Encoder<Encodable> instance = FrameEncoder.create();
            
            @Override
            public Encoder<Encodable> get() {
                return instance;
            }
        }
        
        public static Encoder<Encodable> create() {
            return new FrameEncoder();
        }
        
        private FrameEncoder() {}

        @Override
        public ByteBuf encode(Encodable input, ByteBufAllocator output) throws IOException {
            Frame frame = Frame.fromEncodable(input, output);
            return frame.encode(output);
        }
    }

    public static class FrameDecoder<T> implements Decoder<Optional<T>> {
        
        public static <T> FrameDecoder<T> create(
                Decoder<T> messageDecoder) {
            return new FrameDecoder<T>(messageDecoder);
        }
        
        private final Logger logger = LoggerFactory
                .getLogger(FrameDecoder.class);
        private final Decoder<T> messageDecoder;
        
        private FrameDecoder(
                Decoder<T> messageDecoder) {
            this.messageDecoder = checkNotNull(messageDecoder);
        }
    
        @Override
        public Optional<T> decode(ByteBuf input) throws IOException {
            Optional<T> output; 
            Optional<Frame> optFrame = Frame.decode(input);
            if (optFrame.isPresent()) {
                Frame frame = optFrame.get();
                ByteBuf frameBuffer = frame.buffer();
                int readable = frameBuffer.readableBytes();
                if (readable < frame.length()) {
                    throw new IllegalArgumentException();
                }
                
                output = Optional.of(messageDecoder.decode(frameBuffer));
                
                // make sure we consume the entire frame
                readable = frame.length() - (readable - frameBuffer.readableBytes());
                if (readable > 0) {
                    logger.debug(String.format("Skipping %d unread bytes after %s",
                            readable, output));
                    frameBuffer.skipBytes(readable);
                }
            } else {
                output = Optional.absent();
            }
            return output;
        }
    }

    private static final int MIN_LENGTH = 0; // TODO: or should be 1?
    private static final int MAX_LENGTH = BinaryInputArchive.maxBuffer;
    
    public static int getMinLength() {
        return MIN_LENGTH;
    }

    public static int getMaxLength() {
        return MAX_LENGTH;
    }
    
    public static boolean validLength(int length) {
        return (length >= MIN_LENGTH && length <= MAX_LENGTH);
    }
    
    public static Optional<Frame> decode(ByteBuf input) throws IOException {
        input.markReaderIndex();
        Optional<Header> header = Header.decode(input);
        if (header.isPresent()) {
            int length = header.get().intValue();
            if (! Frame.validLength(length)) {
                input.resetReaderIndex();
                throw new IllegalArgumentException(String.format("Invalid frame length 0x%x", length));
            }
            if (input.readableBytes() >= length) {
                Frame frame = Frame.create(header.get(), input);
                return Optional.of(frame);
            }
        }
        input.resetReaderIndex();
        return Optional.absent();
    }

    private final Header header;
    private final ByteBuf buffer;
    
    private Frame(Header header, ByteBuf buffer) {
        this.header = checkNotNull(header);
        this.buffer = checkNotNull(buffer);
    }
    
    public Header header() {
        return header;
    }
    
    public ByteBuf buffer() {
        return buffer;
    }
    
    public int length() {
        return header.intValue();
    }

    @Override
    public ByteBuf encode(ByteBufAllocator output) throws IOException {
        checkArgument(header.intValue() == buffer.readableBytes());
        return Buffers.composite(output, header.encode(output), buffer);
    }
    
    public static class Header implements Encodable {
    
        public static Header create(int intValue) {
            return new Header(intValue);
        }

        private static final int LENGTH = 4;
    
        public static int length() {
            return LENGTH;
        }
        
        public static Optional<Header> decode(ByteBuf input) {
            if (input.readableBytes() >= Header.length()) {
                int value = input.readInt();
                return Optional.of(Header.create(value));
            }
            return Optional.absent();
        }
        
        private final int intValue;
        
        private Header(int intValue) {
            this.intValue = intValue;
        }
        
        public int intValue() {
            return intValue;
        }

        @Override
        public ByteBuf encode(ByteBufAllocator output) {
            ByteBuf out = output.buffer(length(), length());
            out.writeInt(intValue());
            return out;
        }
    }
}
