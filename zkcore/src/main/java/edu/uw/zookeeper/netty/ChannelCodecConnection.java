package edu.uw.zookeeper.netty;

import java.util.Collection;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.Channel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.Connection;

public class ChannelCodecConnection<I,O,T extends Codec<I,? extends O,? extends I,?>> extends AbstractChannelConnection<I,O,ChannelCodecConnection<I,O,T>> implements CodecConnection<I,O,T,ChannelCodecConnection<I,O,T>> {

    public static <I,O,T extends Codec<I,O,? extends I,? extends O>, C extends Connection<?,?,?>> ParameterizedFactory<Channel, C> factory(
            final Factory<? extends T> codecFactory,
            final ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory) {
        return new ParameterizedFactory<Channel, C>() {
            @Override
            public C get(Channel channel) {
                T codec = codecFactory.get();
                ChannelCodecConnection<I,O,T> connection = ChannelCodecConnection.defaults(codec, channel);
                return connectionFactory.get(connection);
            }
        };
    }

    public static <I,O,T extends Codec<I,O,? extends I,? extends O>> ChannelCodecConnection<I,O,T> defaults(
            T codec,
            Channel channel) {
        Collection<Connection.Listener<? super O>> listeners = Collections.emptySet();
        return withListeners(codec, channel, listeners);
    }

    public static <I,O,T extends Codec<I,O,? extends I,? extends O>> ChannelCodecConnection<I,O,T> withListeners(
            T codec, 
            Channel channel,
            Collection<? extends Connection.Listener<? super O>> listeners) {
        Logger logger = LogManager.getLogger(ChannelCodecConnection.class);
        EventfulHandler<? extends O> eventful = EventfulHandler.withLogger(codec.decodeType(), logger);
        ConnectionStateHandler state = ConnectionStateHandler.withLogger(logger);
        return newInstance(codec, channel, eventful, state, listeners, logger);
    }
    
    public static <I,O,T extends Codec<I,O,? extends I,? extends O>> ChannelCodecConnection<I,O,T> newInstance(
            T codec,
            Channel channel,
            EventfulHandler<? extends O> eventful,
            ConnectionStateHandler state,
            Collection<? extends Connection.Listener<? super O>> listeners,
            Logger logger) {
        return new ChannelCodecConnection<I,O,T>(codec, channel, eventful, state, listeners, logger);
    }
    
    protected final T codec;
    
    protected ChannelCodecConnection(
            T codec,
            Channel channel,
            EventfulHandler<? extends O> eventful,
            ConnectionStateHandler state,
            Collection<? extends Connection.Listener<? super O>> listeners,
            Logger logger) {
        super(channel, eventful, state, listeners, logger);
        this.codec = codec;
        
        DecoderHandler.toPipeline(
                DecoderHandler.withDecoder(codec, logger),
                EncoderHandler.toPipeline(
                        EncoderHandler.withEncoder(codec, logger),
                        channel.pipeline()));
    }

    @Override
    public T codec() {
        return codec;
    }

    @Override
    protected ChannelCodecConnection<I,O,T> self() {
        return this;
    }
}
