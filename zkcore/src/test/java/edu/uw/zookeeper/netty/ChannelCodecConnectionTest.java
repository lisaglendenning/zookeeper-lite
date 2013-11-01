package edu.uw.zookeeper.netty;

import io.netty.channel.Channel;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import edu.uw.zookeeper.net.QueueingConnectionListener;
import edu.uw.zookeeper.net.StringCodec;

@RunWith(JUnit4.class)
public class ChannelCodecConnectionTest extends ChannelConnectionTest {

    @Override
    protected QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>> newConnection(Channel channel) {
        QueueingConnectionListener<String> listener = QueueingConnectionListener.linkedQueues();
        ChannelCodecConnection<String,String,?> connection = 
                ChannelCodecConnection.withListeners(
                        StringCodec.defaults(), channel, ImmutableList.of(listener));
        return QueuedConnection.create(connection, listener);
    }
}
