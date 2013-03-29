package org.apache.zookeeper.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class BufSink extends ChannelOutboundMessageHandlerAdapter<ByteBuf> {
    public BlockingQueue<ByteBuf> queue = new LinkedBlockingQueue<ByteBuf>();
    @Override
    public void flush(ChannelHandlerContext ctx, ByteBuf msg)
            throws Exception {
        msg.retain();
        queue.add(msg);
    } 
}