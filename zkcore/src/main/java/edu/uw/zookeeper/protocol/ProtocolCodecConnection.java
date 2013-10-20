package edu.uw.zookeeper.protocol;

import net.engio.mbassy.listener.Handler;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.ProtocolState;

public class ProtocolCodecConnection<I, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> extends ForwardingConnection<I> {

    public static <I, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, ProtocolCodecConnection<I,T,C>> factory() { 
        return new ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, ProtocolCodecConnection<I,T,C>>() {
            @Override
            public ProtocolCodecConnection<I,T,C> get(Pair<? extends Pair<Class<I>, ? extends T>, C> value) {
                return ProtocolCodecConnection.<I,T,C>newInstance(
                        value.first().second(),
                        value.second());
            }
        };    
    }
    
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
            flush();
            read();
            close();
        }
    }

    @Override
    protected C delegate() {
        return connection;
    }
}