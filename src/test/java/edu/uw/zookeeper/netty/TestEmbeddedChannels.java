package edu.uw.zookeeper.netty;

import static org.junit.Assert.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.AbstractEmbeddedChannel;
import io.netty.channel.embedded.EmbeddedByteChannel;
import io.netty.channel.embedded.EmbeddedMessageChannel;

public class TestEmbeddedChannels {

    protected void writeOutbound(AbstractEmbeddedChannel<?> channel, Object msg) {
        boolean written = channel.writeOutbound(msg);
        assertTrue(written);
        Object output = channel.readOutbound();
        assertNull(output);
    }

    protected ByteBuf writeOutboundAndRead(EmbeddedByteChannel channel,
            Object msg) {
        boolean written = channel.writeOutbound(msg);
        assertTrue(written);
        ByteBuf output = channel.readOutbound();
        assertNotNull(output);
        return output;
    }

    protected <T> T writeOutboundAndRead(EmbeddedMessageChannel channel,
            Object msg) {
        boolean written = channel.writeOutbound(msg);
        assertTrue(written);
        @SuppressWarnings("unchecked")
        T output = (T) channel.readOutbound();
        assertNotNull(output);
        return output;
    }

    protected <O> void writeInbound(AbstractEmbeddedChannel<O> channel, O msg) {
        boolean written = channel.writeInbound(msg);
        assertFalse(written);
        Object output = channel.readInbound();
        assertNull(output);
    }

    protected <T, O> T writeInboundAndRead(AbstractEmbeddedChannel<O> channel,
            O msg) {
        boolean written = channel.writeInbound(msg);
        assertTrue(written);
        @SuppressWarnings("unchecked")
        T output = (T) channel.readInbound();
        assertNotNull(output);
        return output;
    }

}
