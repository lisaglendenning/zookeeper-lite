package org.apache.zookeeper.protocol.netty;

import static org.junit.Assert.*;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.apache.zookeeper.util.EventfulEventBus;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;

@RunWith(JUnit4.class)
public class LocalChannelTest {

    protected static final Logger logger = LoggerFactory.getLogger(LocalChannelTest.class);
    
    public static class Module extends LocalModule {

        public static Injector injector;
        
        public static void createInjector() {
            injector = Guice.createInjector(
                    EventfulEventBus.EventfulModule.get(),
                    Module.get());
        }

        public static Module get() {
            return new Module();
        }
        
        @Override
        protected void configure() {
        }
        
        @Provides
        public BufEventTestInitializer newChannelInitializer() {
            return new BufEventTestInitializer(injector.getInstance(ChannelGroup.class));
        }
    }

    @ChannelHandler.Sharable
    public static class ChannelGroupInitializer extends ChannelInitializer<Channel> {
        
        public ChannelGroup channels;
        public Map<SocketAddress, Channel> byAddress = Maps.newHashMap();
        
        @Inject
        public ChannelGroupInitializer(ChannelGroup channels) {
            this.channels = channels;
        }
        
        @Override
        protected void initChannel(Channel channel) throws Exception {
            channels.add(channel);
            byAddress.put(channel.remoteAddress(), channel);
        }
    }
    
    public static class InboundBridge extends ChannelInboundMessageHandlerAdapter<ByteBuf> {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, ByteBuf msg)
                throws Exception {
            ctx.nextInboundByteBuffer().writeBytes(msg);
            ctx.fireInboundBufferUpdated();
        }
        
    }
    
    public static class BufEventSink extends ChannelInboundMessageHandlerAdapter<BufEvent> {

        public BlockingQueue<ByteBuf> queue = new LinkedBlockingQueue<ByteBuf>();
        
        @Override
        public void messageReceived(ChannelHandlerContext ctx, BufEvent msg)
                throws Exception {
            ByteBuf buf = msg.getBuf();
            queue.put(buf.copy());
            buf.skipBytes(buf.readableBytes());
            msg.getCallback().onSuccess(null);
            assertEquals(
                    ctx.pipeline()
                        .context(BufEventDecoder.class)
                        .inboundByteBuffer().readableBytes(), 0);
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,
                Throwable cause) throws Exception {
            ctx.close();
            fail(cause.toString());
        }
    }
    
    public static class BufEventSource extends MessageToMessageEncoder<ByteBuf> {

        public Set<ByteBuf> outstanding = Sets.newHashSet();
        
        public class Callback implements FutureCallback<Void> {

            public ByteBuf buf;
            
            public Callback(ByteBuf buf) {
                this.buf = buf;
                buf.retain();
                outstanding.add(buf);
            }
            
            @Override
            public void onSuccess(Void result) {
                outstanding.remove(buf);
                buf.release();
            }

            @Override
            public void onFailure(Throwable t) {
                fail(t.toString());
            }
        }
        
        @Override
        protected Object encode(ChannelHandlerContext ctx, ByteBuf msg)
                throws Exception {
            BufEvent event = new BufEvent().setBuf(msg.slice()).setCallback(new Callback(msg));
            return event;
        }
        
    }

    @ChannelHandler.Sharable
    public static class BufEventTestInitializer extends ChannelGroupInitializer {

        @Inject
        public BufEventTestInitializer(ChannelGroup channels) {
            super(channels);
        }
        
        @Override
        protected void initChannel(Channel channel) throws Exception {
            super.initChannel(channel);
            //channel.config().setAutoRead(false);
            channel.pipeline().addLast(
                    LoggingDecoder.create(),
                    new InboundBridge(),
                    BufEventEncoder.create(),
                    BufEventDecoder.create(),
                    new BufEventSource(),
                    new BufEventSink());
        }
    }    

    @BeforeClass
    public static void createInjector() {
        Module.createInjector();
    }

    protected static Random RANDOM = new Random();
    protected static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }
    
    @Test(timeout=1000)
    @Ignore
    public void test() throws InterruptedException {
        Injector injector = Module.injector;
        
        BufEventTestInitializer serverHandler = injector.getInstance(BufEventTestInitializer.class);
        ServerBootstrap serverBootstrap = injector.getInstance(ServerBootstrap.class).childHandler(serverHandler);
        ServerChannel server = (ServerChannel) serverBootstrap.bind().sync().channel();
        
        BufEventTestInitializer clientHandler = injector.getInstance(BufEventTestInitializer.class);
        Bootstrap clientBootstrap = injector.getInstance(Bootstrap.class).handler(clientHandler);
        
        Channel clientChannel = clientBootstrap.connect(server.localAddress()).sync().channel();
        Channel serverChannel = serverHandler.byAddress.get(clientChannel.localAddress());
        assertNotNull(serverChannel);
        
        int length = 4;
        byte[] inputData = randomBytes(length);
        ByteBuf buf = Unpooled.wrappedBuffer(inputData);
        buf.retain();
        clientChannel.write(buf);
        ChannelFuture channelFuture = clientChannel.flush().sync();
        assertTrue(channelFuture.isSuccess());
        assertTrue(clientChannel.pipeline().get(BufEventSource.class).outstanding.isEmpty());
        assertTrue(clientChannel.isActive());

        byte[] outputData = new byte[inputData.length];
        serverChannel.read();
        assertTrue(serverChannel.isActive());
        BlockingQueue<ByteBuf> serverQ = serverChannel.pipeline().get(BufEventSink.class).queue;
        ByteBuf receivedBuf = serverQ.take();
        assertTrue(serverQ.isEmpty());
        assertEquals(receivedBuf.readableBytes(), outputData.length);
        receivedBuf.readBytes(outputData);
        assertTrue(Arrays.equals(inputData, outputData));
        
        server.close().sync();
        ChannelGroupFuture groupFuture = serverHandler.channels.close().await();
        assertTrue(groupFuture.isSuccess());
        serverBootstrap.shutdown();
        groupFuture = clientHandler.channels.close().await();
        assertTrue(groupFuture.isSuccess());
        clientBootstrap.shutdown();
    }
}
