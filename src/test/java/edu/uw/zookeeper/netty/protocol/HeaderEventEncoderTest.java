package edu.uw.zookeeper.netty.protocol;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

import edu.uw.zookeeper.Randomizer;
import edu.uw.zookeeper.netty.TestEmbeddedChannels;
import edu.uw.zookeeper.netty.protocol.BufEvent;
import edu.uw.zookeeper.netty.protocol.Header;
import edu.uw.zookeeper.netty.protocol.HeaderEvent;
import edu.uw.zookeeper.netty.protocol.HeaderEventEncoder;

@RunWith(JUnit4.class)
public class HeaderEventEncoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000);

    protected static Randomizer RANDOM = new Randomizer();

    protected static final Logger logger = LoggerFactory
            .getLogger(HeaderEventEncoderTest.class);

    public static class HeaderEventTracker extends HeaderEvent {
        public class Callback implements FutureCallback<Void> {
            @Override
            public void onSuccess(Void result) {
                completed = true;
            }

            @Override
            public void onFailure(Throwable t) {
                fail(t.toString());
            }
        }

        public boolean completed = false;

        public HeaderEventTracker(ByteBuf header, ByteBuf buf) {
            super();
            setHeader(header);
            setBuf(buf);
            setCallback(new Callback());
        }
    }

    @Test
    public void testEncoder() {
        EmbeddedMessageChannel outputChannel = new EmbeddedMessageChannel(
                HeaderEventEncoder.create());
        testLengthEncode(outputChannel, Header.LENGTH);
        testLengthEncode(outputChannel, Header.LENGTH * 2);
        outputChannel.close();
    }

    protected void testLengthEncode(EmbeddedMessageChannel outputChannel,
            int length) {
        // non-empty payload
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        ByteBuf inputHeader = inputBuf.slice(inputBuf.readerIndex(),
                Header.LENGTH);
        inputBuf.skipBytes(Header.LENGTH);
        HeaderEventTracker msg = new HeaderEventTracker(inputHeader, inputBuf);
        BufEvent outputMsg = writeOutboundAndRead(outputChannel, msg);
        readAndValidate(inputData, outputMsg);
        assertFalse(msg.completed);
        outputMsg.getCallback().onSuccess(null);
        assertTrue(msg.completed);
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
