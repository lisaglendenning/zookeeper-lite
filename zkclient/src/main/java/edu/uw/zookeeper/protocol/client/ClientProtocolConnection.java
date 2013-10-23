package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;

public class ClientProtocolConnection<I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> extends ProtocolCodecConnection<I,T,C> {

    public static <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, ClientProtocolConnection<I,T,C>> factory() {
        return new ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, ClientProtocolConnection<I,T,C>>() {
                    @Override
                    public ClientProtocolConnection<I,T,C> get(Pair<? extends Pair<Class<I>, ? extends T>, C> value) {
                        return ClientProtocolConnection.<I,T,C>newInstance(
                                value.first().second(),
                                value.second());
                    }
                };
    }
    
    public static <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> ClientProtocolConnection<I,T,C> newInstance(
            T codec,
            C connection) {
        return new ClientProtocolConnection<I,T,C>(codec, connection);
    }
    
    protected ClientProtocolConnection(
            T codec,
            C connection) {
        super(codec, connection);
    }
}
