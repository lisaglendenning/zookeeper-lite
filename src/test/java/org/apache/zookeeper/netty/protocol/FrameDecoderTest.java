package org.apache.zookeeper.netty.protocol;

import static org.junit.Assert.*;

import java.util.Arrays;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.apache.zookeeper.Randomizer;
import org.apache.zookeeper.netty.TestEmbeddedChannels;
import org.apache.zookeeper.netty.protocol.BufEvent;
import org.apache.zookeeper.netty.protocol.FrameDecoder;
import org.apache.zookeeper.netty.protocol.Header;
import org.apache.zookeeper.netty.protocol.HeaderEventEncoderTest.HeaderEventTracker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class FrameDecoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000); 

    protected static Randomizer RANDOM = new Randomizer();
    
    protected static final Logger logger = LoggerFactory.getLogger(FrameDecoderTest.class);
    
    @Test
    public void testDecoder() {
        EmbeddedMessageChannel inputChannel = new EmbeddedMessageChannel(
                FrameDecoder.create());
        testCompleteDecode(inputChannel, 0);
        testCompleteDecode(inputChannel, 4);
        testChunkedDecode(inputChannel, 4);
        testMultipleDecodes(inputChannel);
        inputChannel.close();
    }
    
    protected void testCompleteDecode(EmbeddedMessageChannel inputChannel, int length) {
        // complete header
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        ByteBuf inputHeader = Unpooled.buffer(Header.LENGTH, Header.LENGTH);
        inputHeader.retain();
        inputHeader.writeInt(length);
        HeaderEventTracker inputMsg = new HeaderEventTracker(inputHeader, inputBuf);
        
        BufEvent outputMsg = writeInboundAndRead(inputChannel, inputMsg);
        readAndValidate(inputData, outputMsg);
        
        assertFalse(inputMsg.completed);
        outputMsg.getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertTrue(inputMsg.completed);
        inputHeader.release();
        inputBuf.release();
    }
    
    protected void testChunkedDecode(EmbeddedMessageChannel inputChannel, int chunk) {
        // chunked payload
        int chunks = 2;
        int length = chunk*chunks;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.buffer(length, length);
        inputBuf.retain();
        ByteBuf inputHeader = Unpooled.buffer(Header.LENGTH, Header.LENGTH);
        inputHeader.retain();
        inputHeader.writeInt(length);
        HeaderEventTracker inputMsg = new HeaderEventTracker(inputHeader, inputBuf);
        
        BufEvent outputMsg = null;
        for (int i=0; i<chunks; ++i) {
            inputBuf.writeBytes(inputData, i*chunk, chunk);
            if (i < chunks - 1) {
                writeInbound(inputChannel, inputMsg);
            } else {
                outputMsg = writeInboundAndRead(inputChannel, inputMsg);
            }
        }
        readAndValidate(inputData, outputMsg);
        
        assertEquals(length, inputBuf.readableBytes());
        assertFalse(inputMsg.completed);
        outputMsg.getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertEquals(0, inputBuf.readableBytes());
        assertTrue(inputMsg.completed);
        inputHeader.release();
        inputBuf.release();
    }

    protected void testMultipleDecodes(EmbeddedMessageChannel inputChannel) {
        int chunk = 4;
        int chunks = 2;
        int length = chunk*chunks;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.buffer(length, length);
        inputBuf.retain();
        ByteBuf inputHeader = Unpooled.buffer(Header.LENGTH, Header.LENGTH);
        inputHeader.retain();
        inputHeader.writeInt(chunk);
        inputHeader.markReaderIndex();
        inputBuf.writeBytes(inputData, 0, chunk);
        HeaderEventTracker[] inputMsgs = {new HeaderEventTracker(inputHeader, inputBuf),
                new HeaderEventTracker(inputHeader, inputBuf)
        };
        BufEvent[] outputMsgs = {null, null};
        outputMsgs[0] = writeInboundAndRead(inputChannel, inputMsgs[0]);
        readAndValidate(Arrays.copyOfRange(inputData, 0, chunk), outputMsgs[0]);
        assertFalse(inputMsgs[0].completed);
        assertEquals(inputBuf.readableBytes(), chunk);
        
        inputHeader.resetReaderIndex();
        inputBuf.writeBytes(inputData, chunk, chunk);
        writeInbound(inputChannel, inputMsgs[1]);

        assertFalse(inputMsgs[1].completed);
        assertEquals(inputBuf.readableBytes(), length);
        outputMsgs[0].getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertTrue(inputMsgs[0].completed);
        assertEquals(inputBuf.readableBytes(), chunk);

        outputMsgs[1] = writeInboundAndRead(inputChannel, inputMsgs[1]);
        readAndValidate(Arrays.copyOfRange(inputData, chunk, chunk*2), outputMsgs[1]);
        
        assertFalse(inputMsgs[1].completed);
        outputMsgs[1].getCallback().onSuccess(null);
        inputChannel.runPendingTasks();
        inputChannel.checkException();
        assertTrue(inputMsgs[1].completed);
        assertEquals(inputBuf.readableBytes(), 0);
        
        inputHeader.release();
        inputBuf.release();
    }
    
    protected void readAndValidate(byte[] inputData, BufEvent msg) {
        assertNotNull(msg);
        ByteBuf payload = msg.getBuf();
        assertNotNull(payload);
        readAndValidate(inputData, payload);
    }

    protected void readAndValidate(byte[] inputData, ByteBuf payload) {
        byte[] outputData = new byte[inputData.length];
        assertEquals(outputData.length, payload.readableBytes());
        payload.readBytes(outputData);
        assertArrayEquals(inputData, outputData);
    }
}
