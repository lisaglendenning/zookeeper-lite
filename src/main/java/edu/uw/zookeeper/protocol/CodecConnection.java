package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import edu.uw.zookeeper.event.ConnectionBufferEvent;
import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.net.Connection;
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
        logger.debug("Writing: {}", message);
        Connection.State connectionState = connection.state();
        switch (connectionState) {
        case CONNECTION_OPENING:
        case CONNECTION_OPENED:
            break;
        default:
            throw new IllegalStateException(connectionState.toString());
        }
        ByteBuf input = codec.encode(message, connection.allocator());
        connection.write(input);
    }

    @Subscribe
    public void handleConnectionState(ConnectionStateEvent event) {
        Connection.State state = event.event().to();
        switch(state) {
        case CONNECTION_CLOSED:
            event.connection().unregister(this);
            break;
        default:
            break;
        }
    }
    
    @Subscribe
    public void handleConnectionRead(ConnectionBufferEvent event) {
        ByteBuf input = event.event();
        Optional<? extends O> output;
        try {
            output = codec.decode(input);
        } catch (Exception e) {
            output = Optional.absent();
            logger.warn("Exception while decoding Connection buffer", e);
            connection.close();
        }
        if (output.isPresent()) {
            post(output.get());
        }
    }
}
