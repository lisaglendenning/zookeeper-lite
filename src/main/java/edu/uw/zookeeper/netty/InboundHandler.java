package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.util.Publisher;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MessageList;

public class InboundHandler extends ChannelInboundHandlerAdapter {

    public static InboundHandler attach(Channel channel, Publisher publisher) {
        InboundHandler handler = newInstance(publisher);
        channel.pipeline().addLast(InboundHandler.class.getName(), handler);
        return handler;
    }
    
    public static InboundHandler newInstance(Publisher publisher) {
        return new InboundHandler(publisher);
    }

    protected final Logger logger;
    protected final Publisher publisher;

    protected InboundHandler(Publisher publisher) {
        super();
        this.logger = LoggerFactory.getLogger(getClass());
        this.publisher = checkNotNull(publisher);
    }
    
    protected void post(Object event) {
        publisher.post(event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageList<Object> input) throws IOException {
        for (int i=0; i<input.size(); ++i) {
            post(input.get(i));
        }
        input.releaseAllAndRecycle();
    }
}
