package org.apache.zookeeper.netty.protocol.client;

import java.util.List;

import org.apache.zookeeper.Xid;
import org.apache.zookeeper.netty.protocol.BufEventDecoder;
import org.apache.zookeeper.netty.protocol.BufEventEncoder;
import org.apache.zookeeper.netty.protocol.ChannelConnection;
import org.apache.zookeeper.netty.protocol.EncodableEncoder;
import org.apache.zookeeper.netty.protocol.FrameDecoder;
import org.apache.zookeeper.netty.protocol.FrameEncoder;
import org.apache.zookeeper.netty.protocol.HeaderEventDecoder;
import org.apache.zookeeper.netty.protocol.HeaderEventEncoder;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class ClientConnection extends ChannelConnection {

    public static class Factory implements Provider<ClientConnection> {
        
        public static Factory get(Provider<Eventful> eventfulFactory, Xid xid) {
            return new Factory(eventfulFactory, xid);
        }
        
        protected Xid xid;
        protected Provider<Eventful> eventfulFactory;
    
        @Inject
        protected Factory(Provider<Eventful> eventfulFactory, Xid xid) {
            this.xid = xid;
            this.eventfulFactory = eventfulFactory;
        }
        
        @Override
        public ClientConnection get() {
            return ClientConnection.create(eventfulFactory, xid);
        }
    }

    @Inject
    public static ClientConnection create(Provider<Eventful> eventfulFactory, Xid xid) {
        return new ClientConnection(eventfulFactory, xid);
    }
    
    protected static List<ChannelHandler> pipeline(Eventful eventful, Xid xid) {
        List<ChannelHandler> pipeline = 
                Lists.<ChannelHandler>newArrayList(
                BufEventEncoder.create(), BufEventDecoder.create(),
                HeaderEventEncoder.create(), HeaderEventDecoder.create(),
                FrameEncoder.create(), FrameDecoder.create(),
                EncodableEncoder.create(),
                OpCallRequestEncodableEncoder.create(),
                ResponseDecoder.create(xid, eventful));
        return pipeline;
    }

    protected final Logger logger = LoggerFactory.getLogger(ClientConnection.class);
    protected final List<ChannelHandler> pipeline;
    
    @Inject
    protected ClientConnection(
            Provider<Eventful> eventfulFactory,
            Xid xid) {
        super(eventfulFactory);
        this.pipeline = pipeline(eventfulBridge, xid);
    }

    @Override
    public void attach(Channel channel) {
        super.attach(channel);
        String name = channel.pipeline().context(channel.pipeline().first()).name();
        for (ChannelHandler handler: pipeline) {
            channel.pipeline().addBefore(name, handler.getClass().getName(), handler);
        }
    }
}
