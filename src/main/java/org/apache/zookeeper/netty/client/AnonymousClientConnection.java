package org.apache.zookeeper.netty.client;

import java.util.List;

import org.apache.zookeeper.netty.ChannelConnection;
import org.apache.zookeeper.netty.protocol.client.FourLetterCommandEncoder;
import org.apache.zookeeper.netty.protocol.client.FourLetterCommandResponseDecoder;
import org.apache.zookeeper.util.Eventful;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class AnonymousClientConnection extends ChannelConnection {

    public static class Factory implements ChannelConnection.Factory<AnonymousClientConnection> {
        
        public static Factory get(Provider<Eventful> eventfulFactory) {
            return new Factory(eventfulFactory);
        }
        
        protected Provider<Eventful> eventfulFactory;
    
        @Inject
        protected Factory(Provider<Eventful> eventfulFactory) {
            this.eventfulFactory = eventfulFactory;
        }
        
        @Override
        public AnonymousClientConnection get(Channel channel) {
            return AnonymousClientConnection.create(channel, eventfulFactory);
        }
    }

    public static AnonymousClientConnection create(
    		Channel channel,
            Provider<Eventful> eventfulFactory) {
    	return new AnonymousClientConnection(channel, eventfulFactory);
    }
    
    protected static List<ChannelHandler> pipeline() {
        return Lists.<ChannelHandler>newArrayList(
                FourLetterCommandEncoder.create(),
                FourLetterCommandResponseDecoder.create());
    }
    
    @Inject
    protected AnonymousClientConnection(
    		Channel channel,
            Provider<Eventful> eventfulFactory) {
    	super(channel, eventfulFactory);
    	
        String name = channel.pipeline().context(channel.pipeline().first()).name();
        for (ChannelHandler handler: pipeline()) {
            channel.pipeline().addBefore(name, handler.getClass().getName(), handler);
        }
    }
    
    protected AnonymousClientConnection(
    		Channel channel,
            Provider<Eventful> eventfulFactory,
            List<ChannelHandler> pipeline) {
        super(channel, eventfulFactory);
        
        String name = channel.pipeline().context(channel.pipeline().first()).name();
        for (ChannelHandler handler: pipeline) {
            channel.pipeline().addBefore(name, handler.getClass().getName(), handler);
        }
    }
}
