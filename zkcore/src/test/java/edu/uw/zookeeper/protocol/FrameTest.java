package edu.uw.zookeeper.protocol;

import static org.junit.Assert.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import edu.uw.zookeeper.net.StringDecoder;
import edu.uw.zookeeper.net.StringEncoder;


@RunWith(JUnit4.class)
public class FrameTest {

    @Test
    public void testFramedCodec() throws IOException {
        Frame.FramedEncoder<String,String> encoder = Frame.FramedEncoder.create(new StringEncoder());
        Frame.FramedDecoder<String,String> decoder = Frame.FramedDecoder.create(
                Frame.FrameDecoder.getDefault(),
                new StringDecoder());
        String input = "hello";
        ByteBuf buf = Unpooled.buffer();
        encoder.encode(input, buf);
        String output = decoder.decode(buf).orNull();
        assertEquals(input, output);
        assertEquals(0, buf.readableBytes());

        encoder.encode(input, buf);
        int readerIndex = buf.readerIndex();
        int writerIndex = buf.writerIndex();
        buf.writerIndex(buf.readerIndex());
        output = decoder.decode(buf).orNull();
        assertEquals(null, output);
        assertEquals(readerIndex, buf.readerIndex());
        
        buf.writerIndex(buf.writerIndex() + 2);
        output = decoder.decode(buf).orNull();
        assertEquals(null, output);
        assertEquals(readerIndex, buf.readerIndex());

        buf.writerIndex(buf.writerIndex() + 2);
        output = decoder.decode(buf).orNull();
        assertEquals(null, output);
        assertEquals(readerIndex, buf.readerIndex());

        buf.writerIndex(buf.writerIndex() + 1);
        output = decoder.decode(buf).orNull();
        assertEquals(null, output);
        assertEquals(readerIndex, buf.readerIndex());

        buf.writerIndex(writerIndex);
        output = decoder.decode(buf).orNull();
        assertEquals(input, output);
        assertEquals(0, buf.readableBytes());
    }
}
