package org.apache.zookeeper.netty.protocol.client;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.zookeeper.protocol.Encodable;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operation.CallRequest;
import org.apache.zookeeper.protocol.client.OpCallRequestEncoder;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

@ChannelHandler.Sharable
public class OpCallRequestEncodableEncoder extends MessageToMessageEncoder<Operation.CallRequest> {

    public static OpCallRequestEncodableEncoder create() {
        return new OpCallRequestEncodableEncoder();
    }
    
    public class EncoderEncodable implements Encodable {

        protected final Operation.CallRequest msg;
        
        public EncoderEncodable(CallRequest msg) {
            super();
            this.msg = msg;
        }

        @Override
        public OutputStream encode(OutputStream stream) throws IOException {
            return encoder.encode(msg, stream);
        }
        
    }
    
    protected final OpCallRequestEncoder encoder;
    
    public OpCallRequestEncodableEncoder() {
        this.encoder = OpCallRequestEncoder.create();
    }
    
    @Override
    protected Object encode(ChannelHandlerContext ctx, Operation.CallRequest msg)
            throws Exception {
        return new EncoderEncodable(msg);
    }
}
