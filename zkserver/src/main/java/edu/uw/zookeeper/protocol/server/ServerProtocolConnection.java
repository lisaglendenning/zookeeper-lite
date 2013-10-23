package edu.uw.zookeeper.protocol.server;

import net.engio.mbassy.listener.Handler;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;

public class ServerProtocolConnection<I, T extends ProtocolCodec<?,?>, C extends Connection<? super I>> extends ProtocolCodecConnection<I,T,C> {

    public static <I, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, ServerProtocolConnection<I,T,C>> factory() { 
        return new ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, ServerProtocolConnection<I,T,C>>() {
            @Override
            public ServerProtocolConnection<I,T,C> get(Pair<? extends Pair<Class<I>, ? extends T>, C> value) {
                return ServerProtocolConnection.<I,T,C>newInstance(
                        value.first().second(),
                        value.second());
            }
        };    
    }
    
    public static <I, T extends ProtocolCodec<?,?>, C extends Connection<? super I>> ServerProtocolConnection<I,T,C> newInstance(
            T codec, 
            C connection) {
        return new ServerProtocolConnection<I,T,C>(codec, connection);
    }
    
    public ServerProtocolConnection(T codec, C connection) {
        super(codec, connection);
    }

    @Handler
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if (event.to() == ProtocolState.CONNECTED) {
            // we now need to trigger a flush of any accumulated read buffer
            connection.read();
        }
        
        super.handleTransitionEvent(event);
    }
}
