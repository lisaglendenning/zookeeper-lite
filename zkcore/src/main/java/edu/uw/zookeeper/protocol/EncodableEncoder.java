package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import edu.uw.zookeeper.common.Singleton;
import edu.uw.zookeeper.net.Encoder;

public class EncodableEncoder implements Encoder<Encodable, Encodable> {
    
    public static EncodableEncoder getInstance() {
        return Holder.INSTANCE.get();
    }
    
    public static enum Holder implements Singleton<EncodableEncoder> {
        INSTANCE(new EncodableEncoder());

        private final EncodableEncoder instance;

        private Holder(EncodableEncoder instance) {
            this.instance = instance;
        }
        
        @Override
        public EncodableEncoder get() {
            return instance;
        }
    }
    
    public EncodableEncoder() {}
    
    @Override
    public Class<? extends Encodable> encodeType() {
        return Encodable.class;
    }

    @Override
    public void encode(Encodable input, ByteBuf output) throws IOException {
        input.encode(output);
    }
}