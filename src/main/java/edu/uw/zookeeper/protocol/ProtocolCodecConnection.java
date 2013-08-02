package edu.uw.zookeeper.protocol;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.ProtocolState;

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
        this.logger = LogManager.getLogger(getClass());
        
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
    
    @Subscribe
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            try {
                delegate().unregister(this);
            } catch (IllegalArgumentException e) {}
        } else if (ProtocolState.DISCONNECTED == event.to() || ProtocolState.ERROR == event.to()) {
            try {
                codec().unregister(this);
            } catch (IllegalArgumentException e) {}
            close();
        }
    }

    @Override
    protected C delegate() {
        return connection;
    }
}