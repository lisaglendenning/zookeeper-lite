package edu.uw.zookeeper.protocol;

import com.google.common.eventbus.Subscribe;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Automaton;

public abstract class ProtocolCodecConnection<I extends Message, O extends Message, T extends ProtocolCodec<I,O>> extends CodecConnection<I, O, T> {

    protected ProtocolCodecConnection(
            T codec, 
            Connection<I> connection) {
        super(codec, connection);
        register(this);
    }

    @SuppressWarnings("unchecked")
    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (event.type().isAssignableFrom(Connection.State.class)) {
            handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
        } else if (event.type().isAssignableFrom(ProtocolState.class)) {
            handleProtocolStateEvent((Automaton.Transition<ProtocolState>)event);
        } 
    }
    
    public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
        switch (event.to()) {
        case CONNECTION_CLOSED:
            try {
                unregister(this);
            } catch (IllegalArgumentException e) {}
            break;
        default:
            break;
        }
    }
    
    public void handleProtocolStateEvent(Automaton.Transition<ProtocolState> event) {
        switch (event.to()) {
        case DISCONNECTED:
        case ERROR:
            close();
            break;
        default:
            break;
        }
    }
}