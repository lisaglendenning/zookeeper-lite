package org.apache.zookeeper.netty.protocol;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.apache.zookeeper.Randomizer;
import org.apache.zookeeper.netty.TestEmbeddedChannels;
import org.apache.zookeeper.netty.protocol.FrameEncoder;
import org.apache.zookeeper.netty.protocol.Header;
import org.apache.zookeeper.netty.protocol.HeaderEvent;
import org.apache.zookeeper.netty.protocol.BufEventEncoderTest.BufEventTracker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class FrameEncoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000); 

    protected static Randomizer RANDOM = new Randomizer();
    
    protected static final Logger logger = LoggerFactory.getLogger(FrameEncoderTest.class);
    
    @Test
    public void testEncoder() {
        EmbeddedMessageChannel outputChannel = new EmbeddedMessageChannel(
                FrameEncoder.create());
        testCompleteEncode(outputChannel, 0);
        testCompleteEncode(outputChannel, 4);
        outputChannel.close();
    }
    
    protected void testCompleteEncode(EmbeddedMessageChannel outputChannel, int length) {
        // non-empty payload
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEventTracker msg = new BufEventTracker(inputBuf);
        HeaderEvent outputMsg = writeOutboundAndRead(outputChannel, msg);
        readAndValidate(inputData, outputMsg);
        assertFalse(msg.completed);
        outputMsg.getCallback().onSuccess(null);
        assertTrue(msg.completed);
        inputBuf.release();
    }

    protected void readAndValidate(byte[] inputData, ByteBuf payload) {
        byte[] outputData = new byte[inputData.length];
        assertEquals(outputData.length, payload.readableBytes());
        payload.readBytes(outputData);
        assertArrayEquals(inputData, outputData);
    }

    protected void readAndValidate(byte[] inputData, HeaderEvent msg) {
        assertNotNull(msg);
        ByteBuf header = msg.getHeader();
        ByteBuf payload = msg.getBuf();
        assertNotNull(header);
        assertNotNull(payload);
        readAndValidate(inputData, header, payload);
    }
    
    protected void readAndValidate(byte[] inputData, ByteBuf header, ByteBuf payload) {
        assertEquals(Header.LENGTH, header.readableBytes());
        int length = header.readInt();
        assertEquals(payload.readableBytes(), length);
        readAndValidate(inputData, payload);
    }
}
