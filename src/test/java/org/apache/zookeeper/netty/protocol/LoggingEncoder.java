package org.apache.zookeeper.netty.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.BufUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

@ChannelHandler.Sharable
public class LoggingEncoder extends MessageToMessageEncoder<ByteBuf> {

    public static LoggingEncoder create() {
        return new LoggingEncoder();
    }
    
    protected final Logger logger;
    
    public LoggingEncoder() { 
        this(LoggerFactory.getLogger(LoggingEncoder.class)); 
    }
    
    public LoggingEncoder(Logger logger) {
        this.logger = logger;
    }
    
    @Override
    protected Object encode(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        logger.debug("{}: {}", ctx.channel(), msg);
        logger.debug(BufUtil.hexDump(msg));
        return msg;
    }
}
