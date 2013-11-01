package edu.uw.zookeeper.netty;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.Collections;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.net.Connection;

public class ChannelConnection<I,O> 
        extends AbstractChannelConnection<I,O,ChannelConnection<I,O>> {

    public static <I,O> ChannelConnection<I,O> defaults(
            Class<? extends O> type,
            Channel channel) {
        Collection<Connection.Listener<? super O>> listeners = Collections.emptySet();
        return withListeners(type, channel, listeners);
    }

    public static <I,O> ChannelConnection<I,O> withListeners(
            Class<? extends O> type,
            Channel channel,
            Collection<? extends Connection.Listener<? super O>> listeners) {
        Logger logger = LogManager.getLogger(ChannelConnection.class);
        EventfulHandler<O> eventful = EventfulHandler.withLogger(type, logger);
        ConnectionStateHandler state = ConnectionStateHandler.withLogger(logger);
        return newInstance(channel, eventful, state, listeners, logger);
    }
    
    public static <I,O> ChannelConnection<I,O> newInstance(
            Channel channel,
            EventfulHandler<? extends O> eventful,
            ConnectionStateHandler state,
            Collection<? extends Connection.Listener<? super O>> listeners,
            Logger logger) {
        return new ChannelConnection<I,O>(channel, eventful, state, listeners, logger);
    }
    
    protected ChannelConnection(
            Channel channel,
            EventfulHandler<? extends O> eventful,
            ConnectionStateHandler state,
            Collection<? extends Connection.Listener<? super O>> listeners,
            Logger logger) {
        super(channel, eventful, state, listeners, logger);
    }

    @Override
    protected ChannelConnection<I,O> self() {
        return this;
    }
}
