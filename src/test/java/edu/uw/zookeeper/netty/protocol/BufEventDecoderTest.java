package edu.uw.zookeeper.netty.protocol;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedByteChannel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.Randomizer;
import edu.uw.zookeeper.netty.TestEmbeddedChannels;
import edu.uw.zookeeper.netty.protocol.BufEvent;
import edu.uw.zookeeper.netty.protocol.BufEventDecoder;

@RunWith(JUnit4.class)
public class BufEventDecoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000);

    protected static Randomizer RANDOM = new Randomizer();

    protected static final Logger logger = LoggerFactory
            .getLogger(BufEventDecoderTest.class);

    @Test
    public void testDecoder() {
        EmbeddedByteChannel inputChannel = new EmbeddedByteChannel(
                BufEventDecoder.create());
        testEmptyDecode(inputChannel);
        testCompleteDecode(inputChannel);
        testPartialDecode(inputChannel);
        testFailedDecode(inputChannel);
        inputChannel.close();
    }

    protected void testEmptyDecode(EmbeddedByteChannel inputChannel) {
        // zero-length buffer shouldn't trigger an event
        int length = 0;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        writeInbound(inputChannel, inputBuf);
        inputBuf.release();
    }

    protected void testCompleteDecode(EmbeddedByteChannel inputChannel) {
        // completely-read buffer shouldn't trigger another event
        int length = 4;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEvent event = writeInboundAndRead(inputChannel, inputBuf);
        byte[] outputData = new byte[inputData.length];
        ByteBuf outputBuf = event.getBuf();
        assertEquals(outputBuf.readableBytes(), outputData.length);
        outputBuf.readBytes(outputData);
        assertArrayEquals(inputData, outputData);
        event.getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        event = (BufEvent) inputChannel.readInbound();
        assertNull(event);
        inputBuf.release();
    }

    protected void testPartialDecode(EmbeddedByteChannel inputChannel) {
        // partially read buffer should trigger another event
        int chunk = 2;
        int length = chunk * 2;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEvent event = writeInboundAndRead(inputChannel, inputBuf);
        byte[] outputData = new byte[length];
        for (int i = 0; i < 2; ++i) {
            assertNotNull(event);
            ByteBuf outputBuf = event.getBuf();
            assertEquals(outputBuf.readableBytes(), (2 - i) * chunk);
            outputBuf.readBytes(outputData, i * chunk, chunk);
            event.getCallback().onSuccess(null);
            inputChannel.runPendingTasks();
            inputChannel.checkException();
            event = (BufEvent) inputChannel.readInbound();
        }
        assertNull(event);
        assertArrayEquals(inputData, outputData);
        inputBuf.release();
    }

    protected void testFailedDecode(EmbeddedByteChannel inputChannel) {
        // failed callback should trigger a channel exception
        int length = 1;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEvent event = writeInboundAndRead(inputChannel, inputBuf);
        Throwable t = new IllegalStateException();
        event.getCallback().onFailure(t);
        inputChannel.runPendingTasks();
        try {
            inputChannel.checkException();
            fail("Expected exception: " + t.toString());
        } catch (Exception e) {
        }
    }
}
