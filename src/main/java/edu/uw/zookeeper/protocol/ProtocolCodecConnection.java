package edu.uw.zookeeper.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Automaton;

public class ProtocolCodecConnection<I, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> extends ForwardingConnection<I> {

    public static <I, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> ProtocolCodecConnection<I,T,C> newInstance(
            T codec, 
            C connection) {
        return new ProtocolCodecConnection<I,T,C>(codec, connection);
    }
    
    protected final Logger logger;
    protected final T codec;
    protected final C connection;
    
    public ProtocolCodecConnection(
            T codec, 
            C connection) {
        this.codec = codec;
        this.connection = connection;
        this.logger = LoggerFactory.getLogger(getClass());
        
        register(this);
    }
    
    public T codec() {
        return codec;
    }
    
    @Override
    public void register(Object handler) {
        codec.register(handler);
        connection.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        codec.unregister(handler);
        try {
            connection.unregister(handler);
        } catch (IllegalArgumentException e) {}
    }
    
    @SuppressWarnings("unchecked")
    @Subscribe
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if (event.type().isAssignableFrom(Connection.State.class)) {
            handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
        } else if (event.type().isAssignableFrom(ProtocolState.class)) {
            handleProtocolStateEvent((Automaton.Transition<ProtocolState>)event);
        } 
    }
    
    protected void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
        switch (event.to()) {
        case CONNECTION_CLOSED:
            try {
                delegate().unregister(this);
            } catch (IllegalArgumentException e) {}
            break;
        default:
            break;
        }
    }
    
    protected void handleProtocolStateEvent(Automaton.Transition<ProtocolState> event) {
        switch (event.to()) {
        case DISCONNECTED:
        case ERROR:
            try {
                codec().unregister(this);
            } catch (IllegalArgumentException e) {}
            close();
            break;
        default:
            break;
        }
    }

    @Override
    protected C delegate() {
        return connection;
    }
}