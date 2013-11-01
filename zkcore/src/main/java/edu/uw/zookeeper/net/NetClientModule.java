package edu.uw.zookeeper.net;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;

public interface NetClientModule {
    
    <I, O, T extends Codec<I, O, ? extends I, ? extends O>, C extends Connection<?,?,?>> Factory<? extends ClientConnectionFactory<C>> getClientConnectionFactory(
            Factory<? extends T> codecFactory,
            ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory);
}
