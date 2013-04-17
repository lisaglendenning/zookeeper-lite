package edu.uw.zookeeper.netty.server;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;

import edu.uw.zookeeper.SessionConnection;
import edu.uw.zookeeper.ZxidCounter;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.protocol.AnonymousHandler;
import edu.uw.zookeeper.netty.protocol.BufEventDecoder;
import edu.uw.zookeeper.netty.protocol.BufEventEncoder;
import edu.uw.zookeeper.netty.protocol.EncodableEncoder;
import edu.uw.zookeeper.netty.protocol.FrameDecoder;
import edu.uw.zookeeper.netty.protocol.FrameEncoder;
import edu.uw.zookeeper.netty.protocol.HeaderEventDecoder;
import edu.uw.zookeeper.netty.protocol.HeaderEventEncoder;
import edu.uw.zookeeper.netty.protocol.server.FourLetterCommandDecoder;
import edu.uw.zookeeper.netty.protocol.server.FourLetterCommandResponseEncoder;
import edu.uw.zookeeper.netty.protocol.server.OpCallReplyEncodableEncoder;
import edu.uw.zookeeper.netty.protocol.server.RequestDecoder;
import edu.uw.zookeeper.util.Eventful;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class ServerConnection extends ChannelConnection {

    public static class Factory implements
            ChannelConnection.Factory<ServerConnection> {

        public static Factory get(Provider<Eventful> eventfulFactory, ZxidCounter zxid) {
            return new Factory(eventfulFactory, zxid);
        }

        protected ZxidCounter zxid;
        protected Provider<Eventful> eventfulFactory;

        @Inject
        protected Factory(Provider<Eventful> eventfulFactory, ZxidCounter zxid) {
            this.zxid = zxid;
            this.eventfulFactory = eventfulFactory;
        }

        @Override
        public ServerConnection get(Channel channel) {
            return ServerConnection.create(channel, eventfulFactory, zxid);
        }
    }

    public static ServerConnection create(Channel channel,
            Provider<Eventful> eventfulFactory, ZxidCounter zxid) {
        return new ServerConnection(channel, eventfulFactory, zxid);
    }

    /*
     * public class UnthrottleCallable implements Callable<Void> { protected
     * final ChannelHandlerContext ctx;
     * 
     * public UnthrottleCallable(ChannelHandlerContext ctx) { this.ctx =
     * checkNotNull(ctx); }
     * 
     * @Override public Void call() throws Exception { ctx.channel().read();
     * ctx.pipeline().fireInboundBufferUpdated(); return null; } }
     */

    protected static List<ChannelHandler> pipeline(Eventful eventful, ZxidCounter zxid) {
        return Lists.<ChannelHandler> newArrayList(BufEventEncoder.create(),
                BufEventDecoder.create(), FourLetterCommandDecoder.create(),
                FourLetterCommandResponseEncoder.create(),
                HeaderEventEncoder.create(), HeaderEventDecoder.create(),
                FrameEncoder.create(), FrameDecoder.create(),
                EncodableEncoder.create(),
                OpCallReplyEncodableEncoder.create(),
                RequestDecoder.create(zxid, eventful));
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ServerConnection.class);

    @Inject
    protected ServerConnection(Channel channel,
            Provider<Eventful> eventfulFactory, ZxidCounter zxid) {
        super(channel, eventfulFactory);

        channel.config().setAutoRead(false);
        String name = channel.pipeline().context(channel.pipeline().first())
                .name();
        for (ChannelHandler handler : pipeline(eventfulBridge, zxid)) {
            channel.pipeline().addBefore(name, handler.getClass().getName(),
                    handler);
        }
        channel.read();
    }

    protected ServerConnection(Channel channel,
            Provider<Eventful> eventfulFactory, List<ChannelHandler> pipeline) {
        super(channel, eventfulFactory);

        channel.config().setAutoRead(false);
        String name = channel.pipeline().context(channel.pipeline().first())
                .name();
        for (ChannelHandler handler : pipeline) {
            channel.pipeline().addBefore(name, handler.getClass().getName(),
                    handler);
        }
        channel.read();
    }

    @Override
    public void post(Object event) {
        if (event instanceof SessionConnection.State) {
            switch ((SessionConnection.State) event) {
            case CONNECTING:
                toConnecting();
                break;
            case CONNECTED:
                toConnected();
                break;
            default:
                break;

            }
        }
        super.post(event);
    }

    protected void toConnecting() {
        // Remove all anonymous handlers
        ChannelHandler handler = channel.pipeline().get(AnonymousHandler.class);
        while (handler != null) {
            channel.pipeline().removeAndForward(handler);
            handler = channel.pipeline().get(AnonymousHandler.class);
        }
    }

    protected void toConnected() {
        // unthrottle
        channel.config().setAutoRead(true);
        channel.read();
        channel.pipeline().fireInboundBufferUpdated();
        // channel.eventLoop().submit(new UnthrottleCallable(ctx));
    }

}
