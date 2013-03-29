package org.apache.zookeeper.protocol.netty.client;

import java.nio.charset.Charset;

import org.apache.zookeeper.protocol.FourLetterCommand;
import org.apache.zookeeper.protocol.netty.AnonymousHandler;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.string.StringDecoder;

@ChannelHandler.Sharable
public class FourLetterCommandResponseDecoder extends StringDecoder 
        implements AnonymousHandler {
    
    public static FourLetterCommandResponseDecoder create() {
        return new FourLetterCommandResponseDecoder();
    }

    public FourLetterCommandResponseDecoder() {
        super(Charset.forName(FourLetterCommand.CHARSET));
    }
}
