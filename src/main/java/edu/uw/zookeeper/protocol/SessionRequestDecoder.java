package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;

import edu.uw.zookeeper.util.Stateful;

public class SessionRequestDecoder implements Stateful<ProtocolState>,
        Decoder<Message.ClientSessionMessage> {
    
    public static SessionRequestDecoder create(
            Stateful<ProtocolState> stateful) {
        return new SessionRequestDecoder(stateful);
    }

    private final Stateful<ProtocolState> stateful;
    
    private SessionRequestDecoder(
            Stateful<ProtocolState> stateful) {
        this.stateful = stateful;
    }

    @Override
    public ProtocolState state() {
        return stateful.state();
    }
    
    @Override
    public Message.ClientSessionMessage decode(ByteBuf input) throws IOException {
        InputStream stream = new ByteBufInputStream(input);
        ProtocolState state = state();
        Message.ClientSessionMessage output;
        switch (state) {
        case ANONYMOUS:
            output = OpCreateSession.Request.decode(stream);
            break;
        case CONNECTING:
        case CONNECTED:
            output = SessionRequestWrapper.decode(stream);
            break;
        default:
            throw new IllegalStateException(state.toString());
        }
        return output;
    }
}