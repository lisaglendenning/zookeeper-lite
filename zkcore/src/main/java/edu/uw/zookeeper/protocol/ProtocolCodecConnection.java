package edu.uw.zookeeper.protocol;

import net.engio.mbassy.listener.Handler;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.ProtocolState;

public abstract class ProtocolCodecConnection<I, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> extends ForwardingConnection<I> {

    protected final Logger logger;
    protected final T codec;
    protected final C connection;
    
    protected ProtocolCodecConnection(
            T codec, 
            C connection) {
        this.codec = codec;
        this.connection = connection;
        this.logger = LogManager.getLogger(getClass());
        
        subscribe(this);
    }
    
    public T codec() {
        return codec;
    }
    
    @Override
    public void subscribe(Object handler) {
        codec.subscribe(handler);
        connection.subscribe(handler);
    }

    @Override
    public boolean unsubscribe(Object handler) {
        boolean unsubscribed = codec.unsubscribe(handler);
        unsubscribed = connection.unsubscribe(handler) || unsubscribed;
        return unsubscribed;
    }
    
    @Handler
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            unsubscribe(this);
        } else if (ProtocolState.DISCONNECTED == event.to() || ProtocolState.ERROR == event.to()) {
            connection.close();
        }
    }

    @Override
    protected C delegate() {
        return connection;
    }
}