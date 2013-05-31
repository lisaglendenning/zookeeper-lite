package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;

import io.netty.buffer.BufType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundByteHandlerAdapter;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.ChannelPipeline;

public class InboundHandler<O> extends ChannelInboundByteHandlerAdapter {

    @ChannelHandler.Sharable
    public static class MessageToByteHandler extends
            ChannelInboundMessageHandlerAdapter<ByteBuf> {

        public static MessageToByteHandler getInstance() {
            return Holder.INSTANCE.get();
        }
                
        public static enum Holder implements Reference<MessageToByteHandler> {
            INSTANCE(new MessageToByteHandler());

            private final MessageToByteHandler instance;
            
            private Holder(MessageToByteHandler instance) {
                this.instance = instance;
            }
            
            @Override
            public MessageToByteHandler get() {
                return instance;
            }
        }
        
        public MessageToByteHandler() { super(); }
        
        @Override
        public void messageReceived(ChannelHandlerContext ctx, ByteBuf msg)
                throws Exception {
            ctx.nextInboundByteBuffer().writeBytes(msg);
            ctx.fireInboundBufferUpdated();
        }
    }

    public static <O> InboundHandler<O> attach(Channel channel, Publisher publisher, Decoder<Optional<? extends O>> decoder) {
        InboundHandler<O> handler = newInstance(publisher, decoder);
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addFirst(InboundHandler.class.getName(), handler);
        // I think that when the underlying channel type is
        // MESSAGE that our byte buffer will be skipped,
        // even if the message is a bytebuf
        if (channel.metadata().bufferType() == BufType.MESSAGE) {
            pipeline.addFirst(
                    MessageToByteHandler.class.getName(),
                    MessageToByteHandler.getInstance());
        }
        return handler;
    }
    
    public static <O> InboundHandler<O> newInstance(Publisher publisher, Decoder<Optional<? extends O>> decoder) {
        return new InboundHandler<O>(publisher, decoder);
    }

    private final Logger logger;
    private final Publisher publisher;
    private final Decoder<Optional<? extends O>> decoder;

    private InboundHandler(Publisher publisher, Decoder<Optional<? extends O>> decoder) {
        super();
        this.logger = LoggerFactory.getLogger(getClass());
        this.publisher = checkNotNull(publisher);
        this.decoder = decoder;
    }
    
    protected void post(Object event) {
        publisher.post(event);
    }

    @Override
    protected void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in) throws IOException {
        while (in.isReadable()) {
            Optional<? extends O> output = decoder.decode(in);
            if (output.isPresent()) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Read {} ({})", output.get(), this);
                }
                post(output.get());
            } else {
                break;
            }
        }
    }
}
