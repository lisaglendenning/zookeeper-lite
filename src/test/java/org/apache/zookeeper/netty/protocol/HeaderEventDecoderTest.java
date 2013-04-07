package org.apache.zookeeper.netty.protocol;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.apache.zookeeper.Randomizer;
import org.apache.zookeeper.netty.TestEmbeddedChannels;
import org.apache.zookeeper.netty.protocol.Header;
import org.apache.zookeeper.netty.protocol.HeaderEvent;
import org.apache.zookeeper.netty.protocol.HeaderEventDecoder;
import org.apache.zookeeper.netty.protocol.BufEventEncoderTest.BufEventTracker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HeaderEventDecoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000);

    protected static Randomizer RANDOM = new Randomizer();

    protected static final Logger logger = LoggerFactory
            .getLogger(HeaderEventDecoderTest.class);

    @Test
    public void testDecoder() {
        EmbeddedMessageChannel inputChannel = new EmbeddedMessageChannel(
                HeaderEventDecoder.create());
        testCompleteDecode(inputChannel, 0);
        testChunkedHeaderDecode(inputChannel);
        testChunkedDecode(inputChannel);
        inputChannel.close();
    }

    protected void testCompleteDecode(EmbeddedMessageChannel inputChannel,
            int length) {
        // complete header
        length += Header.LENGTH;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEventTracker inputMsg = new BufEventTracker(inputBuf);

        HeaderEvent outputMsg = writeInboundAndRead(inputChannel, inputMsg);
        readAndValidate(inputData, outputMsg);

        assertFalse(inputMsg.completed);
        outputMsg.getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertTrue(inputMsg.completed);
        inputBuf.release();
    }

    protected void testChunkedHeaderDecode(EmbeddedMessageChannel inputChannel) {
        // chunked header and payload
        int chunk = Header.LENGTH - 1;
        int chunks = 2;
        int length = chunk * chunks;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEventTracker inputMsg = null;
        HeaderEvent outputMsg = null;
        for (int i = 0; i < chunks; ++i) {
            if (inputMsg != null) {
                assertTrue(inputMsg.completed);
            }
            inputMsg = new BufEventTracker(inputBuf.slice(
                    inputBuf.readerIndex() + chunk * i, chunk));
            if (i < chunks - 1) {
                writeInbound(inputChannel, inputMsg);
            } else {
                outputMsg = writeInboundAndRead(inputChannel, inputMsg);
            }
        }
        readAndValidate(inputData, outputMsg);
        assertFalse(inputMsg.completed);
        outputMsg.getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertTrue(inputMsg.completed);
        inputBuf.release();
    }

    protected void testChunkedDecode(EmbeddedMessageChannel inputChannel) {
        // chunked payload
        int chunk = Header.LENGTH + 1;
        int chunks = 2;
        int length = chunk * chunks;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.buffer(length, length);
        inputBuf.retain();
        BufEventTracker inputMsg = new BufEventTracker(inputBuf);
        HeaderEvent outputMsg = null;
        for (int i = 0; i < chunks; ++i) {
            inputBuf.writeBytes(inputData, i * chunk, chunk);
            outputMsg = writeInboundAndRead(inputChannel, inputMsg);
            assertEquals(chunk * (i + 1) - Header.LENGTH,
                    inputBuf.readableBytes());
        }
        readAndValidate(inputData, outputMsg);
        assertFalse(inputMsg.completed);
        outputMsg.getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertTrue(inputMsg.completed);
        inputBuf.release();
    }

    protected void readAndValidate(byte[] inputData, HeaderEvent msg) {
        assertNotNull(msg);
        ByteBuf header = msg.getHeader();
        ByteBuf payload = msg.getBuf();
        assertNotNull(header);
        assertNotNull(payload);
        readAndValidate(inputData, header, payload);
    }

    protected void readAndValidate(byte[] inputData, ByteBuf header,
            ByteBuf payload) {
        assertEquals(Header.LENGTH, header.readableBytes());
        assertEquals(inputData.length - Header.LENGTH, payload.readableBytes());
        byte[] outputData = new byte[inputData.length];
        header.readBytes(outputData, 0, Header.LENGTH);
        payload.readBytes(outputData, Header.LENGTH, inputData.length
                - Header.LENGTH);
        assertArrayEquals(inputData, outputData);
    }
}
