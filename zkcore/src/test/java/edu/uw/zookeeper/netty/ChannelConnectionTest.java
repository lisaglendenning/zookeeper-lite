package edu.uw.zookeeper.netty;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Set;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.local.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionTestAdapter;
import edu.uw.zookeeper.net.QueueingConnectionListener;

@RunWith(JUnit4.class)
public class ChannelConnectionTest extends ConnectionTestAdapter {

    @Test(timeout=5000)
    public void test() throws Exception {
        final Set<QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>>> serverConnections =
                Collections.synchronizedSet(Sets.<QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>>>newHashSet());
        final Set<QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>>> clientConnections =
                Collections.synchronizedSet(Sets.<QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>>>newHashSet());
        
        LocalEventLoopGroup group = new LocalEventLoopGroup(1);
        LocalServerChannel server = (LocalServerChannel) 
                new ServerBootstrap()
                    .group(group, group)
                    .channel(LocalServerChannel.class)
                    .localAddress(LocalAddress.ANY)
                    .childHandler(new ChannelInitializer<LocalChannel>() {
                        @Override
                        protected void initChannel(LocalChannel channel)
                                throws Exception {
                            serverConnections.add(newConnection(channel));
                        }
                    })
                    .bind().sync().channel();
        Bootstrap clients = new Bootstrap()
            .group(group)
            .channel(LocalChannel.class)
            .handler(new ChannelInitializer<LocalChannel>() {
                @Override
                protected void initChannel(LocalChannel channel)
                        throws Exception {
                    clientConnections.add(newConnection(channel));
                }
            });
        clients.connect(server.localAddress()).sync();
        
        QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>> clientConnection = Iterables.getOnlyElement(clientConnections);
        QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>> serverConnection = Iterables.getOnlyElement(serverConnections);

        pingPong(clientConnection, serverConnection);
        
        assertEquals(clientConnection.listener().states().take().to(), Connection.State.CONNECTION_OPENED);
        assertEquals(serverConnection.listener().states().take().to(), Connection.State.CONNECTION_OPENED);
        
        server.close().await();
        synchronized (serverConnections) {
            synchronized (clientConnections) {
                for (QueuedConnection<?,?,?> c: Sets.union(serverConnections, clientConnections)) {
                    c.connection().close().get();
                }
            }
        }
        group.shutdownGracefully();

        synchronized (serverConnections) {
            synchronized (clientConnections) {
                for (QueuedConnection<?,?,?> c: Sets.union(serverConnections, clientConnections)) {
                    assertEquals(c.listener().states().take().to(), Connection.State.CONNECTION_CLOSING);
                    assertEquals(c.listener().states().take().to(), Connection.State.CONNECTION_CLOSED);
                    assertTrue(c.listener().states().isEmpty());
                    assertTrue(c.listener().reads().isEmpty());
                }
            }
        }
    }
    
    protected QueuedConnection<String,String,? extends AbstractChannelConnection<String,String,?>> newConnection(Channel channel) {
        QueueingConnectionListener<String> listener = QueueingConnectionListener.linkedQueues();
        ChannelConnection<String,String> connection = 
                ChannelConnection.withListeners(
                        String.class, channel, ImmutableList.of(listener));
        return QueuedConnection.create(connection, listener);
    }
}
