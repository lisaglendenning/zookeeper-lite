package edu.uw.zookeeper.net;

import net.engio.mbassy.PubSubSupport;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.protocol.Codec;

public interface NetClientModule {
    
    <I, T extends Codec<? super I, ? extends Optional<?>>, C extends Connection<?>> Factory<? extends ClientConnectionFactory<C>> getClientConnectionFactory(
            ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<I>, ? extends T>> codecFactory,
            ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, Connection<I>>, C> connectionFactory);
}
