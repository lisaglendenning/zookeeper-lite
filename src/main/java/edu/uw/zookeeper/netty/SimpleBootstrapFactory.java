package edu.uw.zookeeper.netty;

import java.util.Map;
import java.util.Map.Entry;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;

public class SimpleBootstrapFactory implements ParameterizedFactory<Factory<? extends EventLoopGroup>, Bootstrap> {

    public static SimpleBootstrapFactory newInstance(
            Class<? extends Channel> channelType,
            Map<ChannelOption<?>, ?> channelOptions) {
        return new SimpleBootstrapFactory(channelType, channelOptions);
    }
    
    private final Class<? extends Channel> channelType;
    private final Map<ChannelOption<?>, ?> channelOptions;
    
    @SuppressWarnings("unchecked")
    public static <T> Bootstrap option(Bootstrap bootstrap, ChannelOption<T> key, Object value) {
        return bootstrap.option(key, (T)value);
    }
    
    protected SimpleBootstrapFactory(
            Class<? extends Channel> channelType,
            Map<ChannelOption<?>, ?> channelOptions) {
        this.channelType = channelType;
        this.channelOptions = channelOptions;
    }
    
    @Override
    public Bootstrap get(Factory<? extends EventLoopGroup> eventLoopGroupFactory) {
        Bootstrap bootstrap = new Bootstrap()
            .channel(channelType)
            .group(eventLoopGroupFactory.get());

        for (Entry<ChannelOption<?>, ? extends Object> entry : channelOptions.entrySet()) {
            option(bootstrap, entry.getKey(), entry.getValue());
        }
        
        return bootstrap;
    }
}
