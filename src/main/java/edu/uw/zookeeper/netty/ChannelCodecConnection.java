package edu.uw.zookeeper.netty;

import com.google.common.base.Optional;

import io.netty.channel.Channel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Codec;

public class ChannelCodecConnection<I> extends ChannelConnection<I> {
    
    public static <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> FromCodecFactory<I,T,C> factory(
            Factory<? extends Publisher> publisherFactory,
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        return FromCodecFactory.newInstance(publisherFactory, codecFactory, connectionFactory);
    }

    public static class FromCodecFactory<I, T extends Codec<? super I, ? extends Optional<?>>, C extends Connection<?>> implements ParameterizedFactory<Channel, C> {

        public static <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> FromCodecFactory<I,T,C> newInstance(
                Factory<? extends Publisher> publisherFactory,
                ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
                ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
            return new FromCodecFactory<I,T,C>(publisherFactory, codecFactory, connectionFactory);
        }
        
        private final Factory<? extends Publisher> publisherFactory;
        private final ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory;
        private final ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory;
        
        private FromCodecFactory(
                Factory<? extends Publisher> publisherFactory,
                ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
                ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
            super();
            this.publisherFactory = publisherFactory;
            this.codecFactory = codecFactory;
            this.connectionFactory = connectionFactory;
        }

        @Override
        public C get(Channel channel) {
            Publisher publisher = publisherFactory.get();
            ChannelCodecConnection<I> connection = new ChannelCodecConnection<I>(channel, publisher);
            Pair<Class<I>,T> codec = codecFactory.get(connection);
            connection.attach(codec.first(), codec.second());
            return connectionFactory.get(Pair.<Pair<Class<I>, T>, Connection<I>>create(codec, connection));
        }
    }

    protected ChannelCodecConnection(
            Channel channel, 
            Publisher publisher) {
        super(channel, publisher);
    }
    
    protected void attach(
            Class<I> inputType,
            Codec<? super I, ? extends Optional<?>> codec) {
        DecoderHandler.attach(channel, codec, logger);
        EncoderHandler.attach(channel, inputType, codec, logger);
        attach();
    }
}