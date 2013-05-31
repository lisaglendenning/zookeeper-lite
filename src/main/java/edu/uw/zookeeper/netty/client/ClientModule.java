package edu.uw.zookeeper.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class ClientModule {

    public static <I,O, C extends Connection<I>> ParameterizedFactory<Connection.CodecFactory<I,O,C>, Factory<ChannelClientConnectionFactory<I,C>>> factory(
            final RuntimeModule runtime) {
        final Factory<Publisher> publisherFactory = runtime.publisherFactory();
        final Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor());
        return new ParameterizedFactory<Connection.CodecFactory<I,O,C>, Factory<ChannelClientConnectionFactory<I,C>>>() {
            @Override
            public Factory<ChannelClientConnectionFactory<I,C>> get(Connection.CodecFactory<I,O,C> value) {
                ParameterizedFactory<Channel, C> connectionFactory = 
                        ChannelConnection.factory(publisherFactory, value);
                return ChannelClientConnectionFactory.factory(
                        publisherFactory, connectionFactory, bootstrapFactory);
            }
        };
    }
}
