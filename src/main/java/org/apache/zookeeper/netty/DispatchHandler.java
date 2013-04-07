package org.apache.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.util.AttributeKey;

@ChannelHandler.Sharable
public class DispatchHandler extends
        ChannelInboundMessageHandlerAdapter<Object> {

    public static final String EVENTFUL_ATTRIBUTE_NAME = Eventful.class
            .getName();
    public static final AttributeKey<Eventful> EVENTFUL_ATTRIBUTE_KEY = new AttributeKey<Eventful>(
            EVENTFUL_ATTRIBUTE_NAME);

    public static Eventful getEventful(Channel channel) {
        return checkNotNull(channel).attr(EVENTFUL_ATTRIBUTE_KEY).get();
    }

    protected final Logger logger = LoggerFactory
            .getLogger(DispatchHandler.class);
    protected final Eventful eventful;

    public static DispatchHandler create(Eventful eventful) {
        return new DispatchHandler(eventful);
    }

    public DispatchHandler(Eventful eventful) {
        super();
        this.eventful = checkNotNull(eventful);
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().attr(EVENTFUL_ATTRIBUTE_KEY)
                .compareAndSet(null, eventful)) {
            throw new IllegalArgumentException();
        }
        super.afterAdd(ctx);
    }

    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().attr(EVENTFUL_ATTRIBUTE_KEY)
                .compareAndSet(eventful, null)) {
            throw new IllegalArgumentException();
        }
        super.beforeRemove(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event)
            throws Exception {
        dispatch(event);
        // super.userEventTriggered(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        dispatch(msg);
        // ctx.nextInboundMessageBuffer().add(msg);
    }

    public void dispatch(Object event) {
        eventful.post(event);
    }
}
