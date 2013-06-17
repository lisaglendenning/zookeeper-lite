package edu.uw.zookeeper.protocol;

import static org.junit.Assert.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FrameTest {

    public static class StringCodec implements Codec<String, String> {

        @Override
        public void encode(String input, ByteBuf output) throws IOException {
            output.writeBytes(input.getBytes());
        }

        @Override
        public String decode(ByteBuf input) throws IOException {
            int readable = input.readableBytes();
            byte[] bytes = new byte[readable];
            input.readBytes(bytes);
            return new String(bytes);
        }
    }
    
    @Test
    public void testFramedCodec() throws IOException {
        StringCodec codec = new StringCodec();
        Frame.FramedEncoder<String> encoder = Frame.FramedEncoder.create(codec);
        Frame.FramedDecoder<String> decoder = Frame.FramedDecoder.create(
                Frame.FrameDecoder.getDefault(),
                codec);
        String input = "hello";
        ByteBuf buf = Unpooled.buffer();
        encoder.encode(input, buf);
        String output = decoder.decode(buf).orNull();
        assertEquals(input, output);
    }
}
