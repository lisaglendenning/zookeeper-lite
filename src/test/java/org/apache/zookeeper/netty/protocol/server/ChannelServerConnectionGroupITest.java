package org.apache.zookeeper.netty.protocol.server;

import static org.junit.Assert.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEvent;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.netty.LocalModule;
import org.apache.zookeeper.netty.protocol.ChannelConnection;
import org.apache.zookeeper.netty.protocol.client.ChannelClientConnectionGroup;
import org.apache.zookeeper.netty.protocol.client.ClientConnection;
import org.apache.zookeeper.netty.protocol.server.ChannelServerConnectionGroup;
import org.apache.zookeeper.netty.protocol.server.ServerConnection;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;
import org.apache.zookeeper.server.ServerConnectionGroup;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulEventBus;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

@RunWith(JUnit4.class)
public class ChannelServerConnectionGroupITest {

    //@Rule
    //public Timeout globalTimeout = new Timeout(10000); 

    public static class Module extends LocalModule {

        public static Injector injector;
        
        public static void createInjector() {
            injector = Guice.createInjector(
                    Module.get());
        }
        
        public static Module get() {
            return new Module();
        }
        
        @Override
        protected void configure() {
            bind(Eventful.class).to(EventfulEventBus.class);
            bind(ServerConnectionGroup.class).to(ChannelServerConnectionGroup.class);
            bind(ClientConnectionGroup.class).to(ChannelClientConnectionGroup.class);
        }
        
        @Provides
        public Xid xid() {
            return Xid.create();
        }
        
        @Provides
        public Zxid zxid() {
            return Zxid.create();
        }
        
        @Provides @Singleton
        protected ServerConnection.Factory getServerConnectionFactory(Provider<Eventful> eventfulFactory, Zxid zxid) {
            return ServerConnection.Factory.get(eventfulFactory, zxid);
        }

        @Provides @Singleton
        protected ClientConnection.Factory getClientConnectionFactory(Provider<Eventful> eventfulFactory, Xid xid) {
            return ClientConnection.Factory.get(eventfulFactory, xid);
        }
    }
    
    public static class ConnectionCallback {

        public BlockingQueue<Connection> connections;
        
        public ConnectionCallback() {
            this.connections = new LinkedBlockingQueue<Connection>();
        }
        
        @Subscribe
        public void handle(Connection connection) throws InterruptedException {
            connections.put(connection);
        }
    }
    
    public static class EventSink {

        public BlockingQueue<ConnectionEventValue> events;

        public EventSink() {
            this.events = new LinkedBlockingQueue<ConnectionEventValue>();
        }
        
        @Subscribe
        public void handle(ConnectionEventValue event) throws InterruptedException {
            events.put(event);
        }
    }

    @BeforeClass
    public static void createInjector() {
        Module.createInjector();
    }
    
    @Test
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = Module.injector;
        ConnectionCallback callback = injector.getInstance(ConnectionCallback.class);
        ChannelServerConnectionGroup server = injector.getInstance(ChannelServerConnectionGroup.class);
        server.register(callback);
        server.startAndWait();
        
        ChannelClientConnectionGroup clients = injector.getInstance(ChannelClientConnectionGroup.class);
        clients.startAndWait();
        
        ListenableFuture<Connection> future = clients.connect(server.localAddress());
        Connection clientConnection = future.get();
        assertNotNull(clientConnection);
        assertEquals(clientConnection, clients.get(server.localAddress()));
        Connection serverConnection = callback.connections.take();
        assertEquals(serverConnection, server.get(clientConnection.localAddress()));
        assertTrue(clientConnection.state() == Connection.State.OPENED);
        assertTrue(serverConnection.state() == Connection.State.OPENED);
        
        EventSink serverSink = new EventSink();
        serverConnection.register(serverSink);
        EventSink clientSink = new EventSink();
        clientConnection.register(clientSink);
        
        Operation.Request connectRequest = Operations.Requests.create(Operation.CREATE_SESSION);
        clientConnection.send(connectRequest).get();
        clientConnection.flush().get();
        serverConnection.read();
        
        ConnectionEventValue event = clientSink.events.take();
        assertEquals(SessionConnection.State.CONNECTING, event.event());
        event = serverSink.events.take();
        assertEquals(SessionConnection.State.CONNECTING, event.event());
        event = serverSink.events.take();
        assertTrue(event.event() instanceof Operation.Request);
        assertEquals(connectRequest, ((Operation.Request)event.event()));
        
        Operation.Response connectResponse = Operations.Responses.create(Operation.CREATE_SESSION);
        serverConnection.send(connectResponse).get();
        serverConnection.flush().get();
        clientConnection.read();

        event = serverSink.events.take();
        assertEquals(SessionConnection.State.ERROR, event.event());
        event = clientSink.events.take();
        assertEquals(SessionConnection.State.ERROR, event.event());
        event = clientSink.events.take();
        assertTrue(event.event() instanceof Operation.Result);
        Operation.Result result = (Operation.Result)event.event();
        assertEquals(connectRequest, result.request());
        assertEquals(connectResponse, result.response());
        
        clients.stopAndWait();
        server.stopAndWait();

        while (! serverSink.events.isEmpty()) {
            event = serverSink.events.take();
        }
        assertEquals(Connection.State.CLOSED, event.event());
        while (! clientSink.events.isEmpty()) {
            event = clientSink.events.take();
        }
        assertEquals(Connection.State.CLOSED, event.event());

        assertEquals(Connection.State.CLOSED, clientConnection.state());
        assertEquals(Connection.State.CLOSED, serverConnection.state());
    }
}
