package edu.uw.zookeeper.netty.protocol.server;

import java.nio.charset.Charset;

import edu.uw.zookeeper.netty.protocol.AnonymousHandler;
import edu.uw.zookeeper.protocol.FourLetterCommand;

import io.netty.buffer.BufType;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.string.StringEncoder;

@ChannelHandler.Sharable
public class FourLetterCommandResponseEncoder extends StringEncoder implements
        AnonymousHandler {

    public static FourLetterCommandResponseEncoder create() {
        return new FourLetterCommandResponseEncoder();
    }

    public FourLetterCommandResponseEncoder() {
        super(BufType.BYTE, Charset.forName(FourLetterCommand.CHARSET));
    }
}
