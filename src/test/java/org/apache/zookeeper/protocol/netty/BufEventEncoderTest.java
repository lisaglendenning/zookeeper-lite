package org.apache.zookeeper.protocol.netty;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.apache.zookeeper.Randomizer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;

@RunWith(JUnit4.class)
public class BufEventEncoderTest extends TestEmbeddedChannels {
    
    @Rule
    public Timeout globalTimeout = new Timeout(1000); 

    protected static Randomizer RANDOM = new Randomizer();
    
    protected static final Logger logger = LoggerFactory.getLogger(BufEventEncoderTest.class);
    
    public static class BufEventTracker extends BufEvent {
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
        
        public BufEventTracker(ByteBuf buf) {
            super();
            setBuf(buf);
            setCallback(new Callback());
        }
    }

    @Test
    public void testEncoder() {
        EmbeddedMessageChannel outputChannel = new EmbeddedMessageChannel(
                BufEventEncoder.create());
        
        int length = 4;
        byte[] inputData = RANDOM.randomBytes(length);
        ByteBuf inputBuf = Unpooled.wrappedBuffer(inputData);
        inputBuf.retain();
        BufEventTracker msg = new BufEventTracker(inputBuf);
        ByteBuf outputBuf = writeOutboundAndRead(outputChannel, msg);
        assertTrue(msg.completed);
        byte[] outputData = new byte[inputData.length];
        assertEquals(outputBuf.readableBytes(), outputData.length);
        outputBuf.readBytes(outputData);
        assertArrayEquals(inputData, outputData);
        inputBuf.release();
        
        outputChannel.close();
    }
}
