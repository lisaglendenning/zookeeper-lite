package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolState;

public class ServerProtocolConnection<V extends ProtocolCodec<?,?,?,?>,T extends CodecConnection<? super Message.Server, ? extends Message.Client, V, ?>> extends ProtocolConnection<Message.Server,Message.Client,V,T,ServerProtocolConnection<V,T>> {

    public static <V extends ProtocolCodec<?,?,?,?>,T extends CodecConnection<? super Message.Server, ? extends Message.Client, V, ?>> ServerProtocolConnection<V,T> newInstance(
            T connection) {
        return new ServerProtocolConnection<V,T>(connection);
    }
    
    public ServerProtocolConnection(T connection) {
        super(connection);
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        if (transition.to() == ProtocolState.CONNECTED) {
            // we now need to trigger a flush of any accumulated read buffer
            connection.read();
        }
        
        super.handleAutomatonTransition(transition);
    }
}
