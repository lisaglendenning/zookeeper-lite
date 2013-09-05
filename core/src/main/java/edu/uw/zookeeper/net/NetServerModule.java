package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.Codec;

public interface NetServerModule {
    <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<C>> getServerConnectionFactory(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory);
}
