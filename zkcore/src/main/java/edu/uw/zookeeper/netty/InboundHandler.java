package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import net.engio.mbassy.PubSubSupport;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class InboundHandler extends ChannelInboundHandlerAdapter {

    public static InboundHandler attach(Channel channel, PubSubSupport<Object> publisher) {
        InboundHandler handler = create(publisher);
        channel.pipeline().addLast(InboundHandler.class.getName(), handler);
        return handler;
    }
    
    public static InboundHandler create(PubSubSupport<Object> publisher) {
        return new InboundHandler(publisher);
    }

    protected final PubSubSupport<Object> publisher;

    protected InboundHandler(PubSubSupport<Object> publisher) {
        super();
        this.publisher = checkNotNull(publisher);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws IOException {
        publisher.publish(message);
        ReferenceCountUtil.release(message);
    }
}
