package edu.uw.zookeeper.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class CodecConnection<I, O, T extends Codec<I,Optional<? extends O>>> extends ForwardingConnection<I> {

    public static <I, O, T extends Codec<I,Optional<? extends O>>, C extends CodecConnection<I,O,T>> CodecFactory<I,O,C> factory(
            final ParameterizedFactory<? super Connection<I>, C> codecConnectionFactory) {
        return new CodecFactory<I,O,C>() {
            @Override
            public Pair<C, T> get(
                    Connection<I> value) {
                C codecConnection = codecConnectionFactory.get(value);
                return Pair.create(codecConnection, codecConnection.codec());
            }
        };
    }

    public static <I, O, T extends Codec<I,Optional<? extends O>>> CodecConnection<I,O,T> newInstance(
            T codec,
            Connection<I> connection) {
        return new CodecConnection<I,O,T>(codec, connection);
    }

    protected final Logger logger;
    protected final T codec;
    protected final Connection<I> connection;

    protected CodecConnection(
            T codec,
            Connection<I> connection) {
        super();
        this.logger = LoggerFactory.getLogger(getClass());
        this.codec = codec;
        this.connection = connection;
        
        connection.register(this);
    }
    
    @Override
    protected Connection<I> delegate() {
        return connection;
    }
    
    public T codec() {
        return codec;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("codec", codec()).add("connection", delegate())
                .toString();
    }
}
