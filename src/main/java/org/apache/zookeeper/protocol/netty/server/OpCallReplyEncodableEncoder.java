package org.apache.zookeeper.protocol.netty.server;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.zookeeper.protocol.Encodable;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.server.OpCallReplyEncoder;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

@ChannelHandler.Sharable
public class OpCallReplyEncodableEncoder extends MessageToMessageEncoder<Operation.CallReply> {

    public static OpCallReplyEncodableEncoder create() {
        return new OpCallReplyEncodableEncoder();
    }
    
    public class EncoderEncodable implements Encodable {

        protected final Operation.CallReply msg;
        
        public EncoderEncodable(Operation.CallReply msg) {
            super();
            this.msg = msg;
        }

        @Override
        public OutputStream encode(OutputStream stream) throws IOException {
            return encoder.encode(msg, stream);
        }
        
    }
    
    protected final OpCallReplyEncoder encoder;
    
    public OpCallReplyEncodableEncoder() {
        this.encoder = OpCallReplyEncoder.create();
    }
    
    @Override
    protected Object encode(ChannelHandlerContext ctx, Operation.CallReply msg)
            throws Exception {
        return new EncoderEncodable(msg);
    }
}
