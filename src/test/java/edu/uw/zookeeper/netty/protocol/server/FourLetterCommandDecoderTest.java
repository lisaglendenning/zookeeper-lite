package edu.uw.zookeeper.netty.protocol.server;

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

import edu.uw.zookeeper.Randomizer;
import edu.uw.zookeeper.netty.TestEmbeddedChannels;
import edu.uw.zookeeper.netty.protocol.BufEventEncoderTest.BufEventTracker;
import edu.uw.zookeeper.netty.protocol.server.FourLetterCommandDecoder;
import edu.uw.zookeeper.protocol.FourLetterCommand;

@RunWith(JUnit4.class)
public class FourLetterCommandDecoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000);

    protected static Randomizer RANDOM = new Randomizer();

    protected static final Logger logger = LoggerFactory
            .getLogger(FourLetterCommandDecoderTest.class);

    @Test
    public void testDecoder() {
        EmbeddedMessageChannel inputChannel = new EmbeddedMessageChannel(
                FourLetterCommandDecoder.create());
        testAllWordsDecode(inputChannel);
        testNonWordDecode(inputChannel);
        inputChannel.close();
    }

    protected void testAllWordsDecode(EmbeddedMessageChannel inputChannel) {
        for (FourLetterCommand command : FourLetterCommand.values()) {
            ByteBuf inputBuf = Unpooled.wrappedBuffer(command.bytes());
            BufEventTracker inputMsg = new BufEventTracker(inputBuf);
            FourLetterCommand outputMsg = writeInboundAndRead(inputChannel,
                    inputMsg);
            assertFalse(inputBuf.isReadable());
            assertTrue(inputMsg.completed);
            assertEquals(command, outputMsg);
        }
    }

    protected void testNonWordDecode(EmbeddedMessageChannel inputChannel) {
        int length = FourLetterCommand.LENGTH;
        byte[] bytes;
        while (true) {
            bytes = RANDOM.randomBytes(length);
            if (!FourLetterCommand.isWord(bytes)) {
                break;
            }
        }
        ByteBuf inputBuf = Unpooled.wrappedBuffer(bytes);
        BufEventTracker inputMsg = new BufEventTracker(inputBuf);
        Object outputMsg = writeInboundAndRead(inputChannel, inputMsg);
        assertSame(inputMsg, outputMsg);
        assertEquals(length, inputBuf.readableBytes());
        assertFalse(inputMsg.completed);
    }
}
