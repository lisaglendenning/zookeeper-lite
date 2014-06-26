package edu.uw.zookeeper.protocol.client;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;

public class AnonymousClientCodec implements Codec<Message.ClientAnonymous, Message.ServerAnonymous, Message.ClientAnonymous, Message.ServerAnonymous> {

    public static AnonymousClientCodec getInstance() {
        return Holder.INSTANCE.get();
    }
    
    public static enum Holder implements Supplier<AnonymousClientCodec> {
        INSTANCE(new AnonymousClientCodec());

        private final AnonymousClientCodec instance;

        private Holder(AnonymousClientCodec instance) {
            this.instance = instance;
        }
        
        @Override
        public AnonymousClientCodec get() {
            return instance;
        }
    }
    
    protected AnonymousClientCodec() {}
    
    @Override
    public Class<Message.ClientAnonymous> encodeType() {
        return Message.ClientAnonymous.class;
    }

    @Override
    public void encode(Message.ClientAnonymous input, ByteBuf output)
            throws IOException {
        input.encode(output);
    }

    @Override
    public Class<Message.ServerAnonymous> decodeType() {
        return Message.ServerAnonymous.class;
    }

    @Override
    public Optional<? extends Message.ServerAnonymous> decode(ByteBuf input)
            throws IOException {
        return FourLetterResponse.decode(input);
    }
}
