package edu.uw.zookeeper.protocol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ProtocolState;

public abstract class ProtocolConnection<I,O,V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>, C extends ProtocolConnection<I,O,V,T,C>> extends ForwardingCodecConnection<I,O,V,T,C> implements Automatons.AutomatonListener<ProtocolState>, Connection.Listener<O> {

    protected final Logger logger;
    protected final T connection;
    
    protected ProtocolConnection(T connection) {
        this.logger = LogManager.getLogger(getClass());
        this.connection = connection;

        connection.subscribe(this);
        connection.codec().subscribe(this);
    }

    @Override
    public void handleConnectionRead(O message) {
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        switch (transition.to()) {
        case DISCONNECTED:
        case ERROR:
        {
            execute(new CloseTask());
            break;
        }
        default:
            break;
        }
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> transition) {
        switch (transition.to()) {
        case CONNECTION_CLOSED:
        {
            delegate().unsubscribe(this);
            codec().unsubscribe(this);
            
            ProtocolState state = codec().state();
            switch (state) {
            case ANONYMOUS:
            case DISCONNECTED:
            case ERROR:
                break;
            default:
                logger.warn(LoggingMarker.PROTOCOL_MARKER.get(), "Connection closed with protocol state {}: {}", state, this);
                codec().apply(ProtocolState.ERROR);
            }
            break;
        }
        default:
            break;
        }
    }

    @Override
    protected T delegate() {
        return connection;
    }
    
    public class CloseTask implements Runnable {
        @Override
        public void run() {
            logger.debug("Closing {}", ProtocolConnection.this);
            delegate().flush();
            delegate().close();
        }
    }
}