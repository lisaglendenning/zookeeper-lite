package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;

public interface NetServerModule {
    <I, O, T extends Codec<I, O, ? extends I, ? extends O>, C extends Connection<?,?,?>> ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<C>> getServerConnectionFactory(
            Factory<? extends T> codecFactory,
            ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory);
}
