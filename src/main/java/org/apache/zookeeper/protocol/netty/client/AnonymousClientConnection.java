package org.apache.zookeeper.protocol.netty.client;

import java.util.List;

import org.apache.zookeeper.protocol.netty.ChannelConnection;
import org.apache.zookeeper.util.Eventful;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

public class AnonymousClientConnection extends ChannelConnection {

    protected static List<ChannelHandler> pipeline() {
        return Lists.<ChannelHandler>newArrayList(
                FourLetterCommandEncoder.create(),
                FourLetterCommandResponseDecoder.create());
    }

    @Inject
    protected AnonymousClientConnection(
            Provider<Eventful> eventfulFactory) {
        super(eventfulFactory);
    }

    @Override
    public void attach(Channel channel) {
        super.attach(channel);
        String name = channel.pipeline().context(channel.pipeline().first()).name();
        for (ChannelHandler handler: pipeline()) {
            channel.pipeline().addBefore(name, handler.getClass().getName(), handler);
        }
    }
}
