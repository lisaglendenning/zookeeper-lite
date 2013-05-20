package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionBufferEvent;
import edu.uw.zookeeper.net.ConnectionStateEvent;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Publisher;

public class CodecConnection<I, O, T extends Codec<I,Optional<? extends O>>> extends ForwardingEventful {

    public static <I, O, T extends Codec<I,Optional<? extends O>>> CodecConnection<I,O,T> create(Publisher publisher,
            T codec,
            Connection connection) {
        return new CodecConnection<I,O,T>(publisher, codec, connection);
    }
    
    protected final Logger logger = LoggerFactory
            .getLogger(CodecConnection.class);
    private final T codec;
    private final Connection connection;

    protected CodecConnection(Publisher publisher,
            T codec,
            Connection connection) {
        super(publisher);
        this.codec = codec;
        this.connection = connection;
        
        connection.register(this);
    }
    
    public Connection asConnection() {
        return connection;
    }
    
    public T asCodec() {
        return codec;
    }
    
    public void write(I message) throws IOException {
        Connection.State connectionState = asConnection().state();
        switch (connectionState) {
        case CONNECTION_OPENING:
        case CONNECTION_OPENED:
            break;
        default:
            throw new IllegalStateException(connectionState.toString());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Encoding {} ({})", message, this);
        }
        ByteBuf input;
        try {
            input = codec.encode(message, asConnection().allocator());
        } catch (Exception e) {
            logger.warn("Exception while encoding {} ({})", message, this, e);
            throw Throwables.propagate(e);
        }
        
        if (logger.isTraceEnabled()) {
            logger.trace("Writing {} ({})", input, this);
        }
        asConnection().write(input);
    }

    @Subscribe
    public void handleConnectionStateEvent(ConnectionStateEvent event) {
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            event.connection().unregister(this);
            break;
        default:
            break;
        }
    }

    @Subscribe
    public void handleConnectionBufferEvent(ConnectionBufferEvent event) {
        if (event.connection() == asConnection()) {
            handleBuffer(event.event());
        }
    }
    
    public void handleBuffer(ByteBuf input) {
        while (input.isReadable()) {
            Optional<? extends O> output;
            try {
                output = codec.decode(input);
            } catch (Exception e) {
                logger.warn("Exception while decoding {} ({})", input, this, e);
                asConnection().close();
                return;
            }
            if (output.isPresent()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Read {} ({})", output.get(), this);
                }
                post(output.get());
            } else {
                break;
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("codec", asCodec()).add("connection", asConnection())
                .toString();
    }
}
