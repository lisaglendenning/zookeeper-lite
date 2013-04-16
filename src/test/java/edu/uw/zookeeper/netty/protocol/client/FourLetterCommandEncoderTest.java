package edu.uw.zookeeper.netty.protocol.client;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.netty.TestEmbeddedChannels;
import edu.uw.zookeeper.netty.protocol.client.FourLetterCommandEncoder;
import edu.uw.zookeeper.protocol.FourLetterCommand;

@RunWith(JUnit4.class)
public class FourLetterCommandEncoderTest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(1000);

    protected static final Logger logger = LoggerFactory
            .getLogger(FourLetterCommandEncoderTest.class);

    @Test
    public void testEncoder() {
        EmbeddedMessageChannel outputChannel = new EmbeddedMessageChannel(
                FourLetterCommandEncoder.create());
        testAllWordsEncode(outputChannel);
        outputChannel.close();
    }

    protected void testAllWordsEncode(EmbeddedMessageChannel outputChannel) {
        for (FourLetterCommand command : FourLetterCommand.values()) {
            ByteBuf outputMsg = writeOutboundAndRead(outputChannel, command);
            byte[] bytes = new byte[outputMsg.readableBytes()];
            outputMsg.readBytes(bytes);
            assertEquals(command, FourLetterCommand.fromWord(bytes));
        }
    }
}
