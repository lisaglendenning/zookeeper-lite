package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Function;

import edu.uw.zookeeper.util.Stateful;

public class SessionReplyDecoder implements Stateful<ProtocolState>, 
        Decoder<Message.ServerSessionMessage> {
    
    public static SessionReplyDecoder create(
            Stateful<ProtocolState> stateful,
            Function<Integer, OpCode> xidToOpCode) {
        return new SessionReplyDecoder(stateful, xidToOpCode);
    }

    private final Stateful<ProtocolState> stateful;
    private final Function<Integer, OpCode> xidToOpCode;
    
    private SessionReplyDecoder(
            Stateful<ProtocolState> stateful,
            Function<Integer, OpCode> xidToOpCode) {
        this.stateful = stateful;
        this.xidToOpCode = xidToOpCode;
    }
    
    @Override
    public ProtocolState state() {
        return stateful.state();
    }
    
    @Override
    public Message.ServerSessionMessage decode(ByteBuf input) throws IOException {
        InputStream stream = new ByteBufInputStream(input);
        ProtocolState state = state();
        Message.ServerSessionMessage output;
        switch (state) {
        case CONNECTING:
            output = OpCreateSession.Response.decode(stream);
            break;
        case CONNECTED:
        case DISCONNECTING:
            output = SessionReplyWrapper.decode(xidToOpCode, stream);
            break;
        default:
            throw new IllegalStateException(state.toString());
        }
        return output;
    }
}