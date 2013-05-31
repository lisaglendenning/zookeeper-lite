package edu.uw.zookeeper.protocol;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;

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

    @Subscribe
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
    
    @Subscribe
    public void handleProtocolStateEvent(Automaton.Transition<ProtocolState> event) {
        switch (event.to()) {
        case DISCONNECTED:
        case ERROR:
            {
                flush().addListener(new Runnable() {
                    @Override
                    public void run() {
                        close();
                    }
                }, MoreExecutors.sameThreadExecutor());
            }
            break;
        default:
            break;
        }
    }
}