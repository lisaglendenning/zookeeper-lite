package edu.uw.zookeeper.netty.client;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;

import edu.uw.zookeeper.XidCounter;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.protocol.BufEventDecoder;
import edu.uw.zookeeper.netty.protocol.BufEventEncoder;
import edu.uw.zookeeper.netty.protocol.EncodableEncoder;
import edu.uw.zookeeper.netty.protocol.FrameDecoder;
import edu.uw.zookeeper.netty.protocol.FrameEncoder;
import edu.uw.zookeeper.netty.protocol.HeaderEventDecoder;
import edu.uw.zookeeper.netty.protocol.HeaderEventEncoder;
import edu.uw.zookeeper.netty.protocol.client.OpCallRequestEncodableEncoder;
import edu.uw.zookeeper.netty.protocol.client.ResponseDecoder;
import edu.uw.zookeeper.util.Eventful;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class ClientConnection extends ChannelConnection {

    public static class Factory implements
            ChannelConnection.Factory<ClientConnection> {

        public static Factory get(Provider<Eventful> eventfulFactory, XidCounter xid) {
            return new Factory(eventfulFactory, xid);
        }

        protected XidCounter xid;
        protected Provider<Eventful> eventfulFactory;

        @Inject
        protected Factory(Provider<Eventful> eventfulFactory, XidCounter xid) {
            this.xid = xid;
            this.eventfulFactory = eventfulFactory;
        }

        @Override
        public ClientConnection get(Channel channel) {
            return ClientConnection.create(channel, eventfulFactory, xid);
        }
    }

    public static ClientConnection create(Channel channel,
            Provider<Eventful> eventfulFactory, XidCounter xid) {
        return new ClientConnection(channel, eventfulFactory, xid);
    }

    protected static List<ChannelHandler> pipeline(Eventful eventful, XidCounter xid) {
        List<ChannelHandler> pipeline = Lists.<ChannelHandler> newArrayList(
                BufEventEncoder.create(), BufEventDecoder.create(),
                HeaderEventEncoder.create(), HeaderEventDecoder.create(),
                FrameEncoder.create(), FrameDecoder.create(),
                EncodableEncoder.create(),
                OpCallRequestEncodableEncoder.create(),
                ResponseDecoder.create(xid, eventful));
        return pipeline;
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ClientConnection.class);

    @Inject
    protected ClientConnection(Channel channel,
            Provider<Eventful> eventfulFactory, XidCounter xid) {
        super(channel, eventfulFactory);

        String name = channel.pipeline().context(channel.pipeline().first())
                .name();
        for (ChannelHandler handler : pipeline(eventfulBridge, xid)) {
            channel.pipeline().addBefore(name, handler.getClass().getName(),
                    handler);
        }
    }

    protected ClientConnection(Channel channel,
            Provider<Eventful> eventfulFactory, List<ChannelHandler> pipeline) {
        super(channel, eventfulFactory);

        String name = channel.pipeline().context(channel.pipeline().first())
                .name();
        for (ChannelHandler handler : pipeline) {
            channel.pipeline().addBefore(name, handler.getClass().getName(),
                    handler);
        }
    }
}
