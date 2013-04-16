package edu.uw.zookeeper.netty.protocol.client;

import java.nio.charset.Charset;

import edu.uw.zookeeper.netty.protocol.AnonymousHandler;
import edu.uw.zookeeper.protocol.FourLetterCommand;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.string.StringDecoder;

@ChannelHandler.Sharable
public class FourLetterCommandResponseDecoder extends StringDecoder implements
        AnonymousHandler {

    public static FourLetterCommandResponseDecoder create() {
        return new FourLetterCommandResponseDecoder();
    }

    public FourLetterCommandResponseDecoder() {
        super(Charset.forName(FourLetterCommand.CHARSET));
    }
}
