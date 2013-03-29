package org.apache.zookeeper.protocol.netty.server;

import java.util.List;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.netty.AnonymousHandler;
import org.apache.zookeeper.protocol.netty.BufEventDecoder;
import org.apache.zookeeper.protocol.netty.BufEventEncoder;
import org.apache.zookeeper.protocol.netty.ChannelConnection;
import org.apache.zookeeper.protocol.netty.FrameDecoder;
import org.apache.zookeeper.protocol.netty.FrameEncoder;
import org.apache.zookeeper.protocol.netty.HeaderEventDecoder;
import org.apache.zookeeper.protocol.netty.HeaderEventEncoder;
import org.apache.zookeeper.protocol.netty.EncodableEncoder;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class ServerConnection extends ChannelConnection {
    
    public static class Factory implements Provider<ServerConnection> {
        
        public static Factory get(Provider<Eventful> eventfulFactory, Zxid zxid) {
            return new Factory(eventfulFactory, zxid);
        }
        
        protected Zxid zxid;
        protected Provider<Eventful> eventfulFactory;

        @Inject
        protected Factory(Provider<Eventful> eventfulFactory, Zxid zxid) {
            this.zxid = zxid;
            this.eventfulFactory = eventfulFactory;
        }
        
        @Override
        public ServerConnection get() {
            return ServerConnection.create(eventfulFactory, zxid);
        }
    }
    
    public static ServerConnection create(Provider<Eventful> eventfulFactory,
            Zxid zxid) {
        return new ServerConnection(eventfulFactory, zxid);
    }
    /*
    public class UnthrottleCallable implements Callable<Void> {
        protected final ChannelHandlerContext ctx;
        
        public UnthrottleCallable(ChannelHandlerContext ctx) {
            this.ctx = checkNotNull(ctx);
        }
    
        @Override
        public Void call() throws Exception {
            ctx.channel().read();
            ctx.pipeline().fireInboundBufferUpdated();
            return null;
        }
    }
*/

    protected static List<ChannelHandler> pipeline(Eventful eventful, Zxid zxid) {
        return Lists.<ChannelHandler>newArrayList(
                BufEventEncoder.create(), BufEventDecoder.create(),
                FourLetterCommandDecoder.create(), FourLetterCommandResponseEncoder.create(),
                HeaderEventEncoder.create(), HeaderEventDecoder.create(),
                FrameEncoder.create(), FrameDecoder.create(),
                EncodableEncoder.create(),
                OpCallReplyEncodableEncoder.create(),
                RequestDecoder.create(zxid, eventful));
    }

    protected final Logger logger = LoggerFactory.getLogger(ServerConnection.class);
    protected final List<ChannelHandler> pipeline;
    
    @Inject
    protected ServerConnection(
            Provider<Eventful> eventfulFactory,
            Zxid zxid) {
        super(eventfulFactory);
        this.pipeline = pipeline(eventfulBridge, zxid);
    }

    @Override
    public void attach(Channel channel) {  
        super.attach(channel);
        channel.config().setAutoRead(false);
        String name = channel.pipeline().context(channel.pipeline().first()).name();
        for (ChannelHandler handler: pipeline) {
            channel.pipeline().addBefore(name, handler.getClass().getName(), handler);
        }
        channel.read();
    }

    @Override
    public void post(Object event) {
        if (event instanceof SessionConnection.State) {
            switch ((SessionConnection.State)event) {
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
//        channel.eventLoop().submit(new UnthrottleCallable(ctx));
    }

}
