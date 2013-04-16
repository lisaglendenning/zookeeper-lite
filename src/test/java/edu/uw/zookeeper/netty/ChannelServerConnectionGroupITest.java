package edu.uw.zookeeper.netty;

import static org.junit.Assert.*;
import java.util.concurrent.ExecutionException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.EventSink;
import edu.uw.zookeeper.SessionConnection;
import edu.uw.zookeeper.Xid;
import edu.uw.zookeeper.Zxid;
import edu.uw.zookeeper.client.ClientConnectionGroup;
import edu.uw.zookeeper.data.OpCreateSessionAction;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.event.ConnectionEvent;
import edu.uw.zookeeper.event.ConnectionMessageEvent;
import edu.uw.zookeeper.event.ConnectionSessionStateEvent;
import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.netty.client.ChannelClientConnectionGroup;
import edu.uw.zookeeper.netty.client.ClientConnection;
import edu.uw.zookeeper.netty.server.ChannelServerConnectionGroup;
import edu.uw.zookeeper.netty.server.ServerConnection;
import edu.uw.zookeeper.server.ServerConnectionGroup;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.EventfulEventBus;

@RunWith(JUnit4.class)
public class ChannelServerConnectionGroupITest {

    @Rule
    public Timeout globalTimeout = new Timeout(10000);

    public static class Module extends LocalModule {

        public static Injector injector;

        public static void createInjector() {
            injector = Guice.createInjector(Module.get());
        }

        public static Module get() {
            return new Module();
        }

        @Override
        protected void configure() {
            super.configure();
            bind(Eventful.class).to(EventfulEventBus.class);
            bind(Xid.class).in(Singleton.class);
            bind(Zxid.class).in(Singleton.class);
            bind(ServerConnectionGroup.class).to(
                    ChannelServerConnectionGroup.class);
            bind(ClientConnectionGroup.class).to(
                    ChannelClientConnectionGroup.class);
            bind(ServerConnection.Factory.class).in(Singleton.class);
            bind(ClientConnection.Factory.class).in(Singleton.class);
        }
    }

    public static class EventfulSink extends EventSink {
        @Subscribe
        public void handle(Connection event) throws InterruptedException {
            put(Connection.class, event);
        }

        @Subscribe
        public void handle(ConnectionEvent event) throws InterruptedException {
            put(event);
        }
    }

    @BeforeClass
    public static void createInjector() {
        Module.createInjector();
    }

    @Test
    public void testInvalidConnect() throws InterruptedException,
            ExecutionException {
        Injector injector = Module.injector;

        EventfulSink[] sinks = { new EventfulSink(), new EventfulSink() };
        EventfulSink clientSink = sinks[0];
        EventfulSink serverSink = sinks[1];

        ChannelServerConnectionGroup server = injector
                .getInstance(ChannelServerConnectionGroup.class);
        server.register(serverSink);
        server.startAndWait();

        ChannelClientConnectionGroup clients = injector
                .getInstance(ChannelClientConnectionGroup.class);
        clients.register(clientSink);
        clients.startAndWait();

        Connection clientConnection = clients.connect(server.localAddress())
                .get();
        assertNotNull(clientConnection);
        assertEquals(clientConnection, Iterables.getOnlyElement(clients));
        assertEquals(clientConnection, clientSink.take(Connection.class));
        assertTrue(clientConnection.state() == Connection.State.CONNECTION_OPENING
                || clientConnection.state() == Connection.State.CONNECTION_OPENED);
        clientConnection.register(clientSink);

        Connection serverConnection = serverSink.take(Connection.class);
        assertEquals(serverConnection, Iterables.getOnlyElement(server));
        assertTrue(serverConnection.state() == Connection.State.CONNECTION_OPENING
                || serverConnection.state() == Connection.State.CONNECTION_OPENED);
        serverConnection.register(serverSink);

        Operation.Request connectRequest = Operations.Requests
                .create(Operation.CREATE_SESSION);
        clientConnection.send(connectRequest).get();
        clientConnection.flush().get();
        serverConnection.read();

        for (EventfulSink sink : sinks) {
            ConnectionSessionStateEvent sessionStateEvent = sink
                    .take(ConnectionSessionStateEvent.class);
            assertEquals(SessionConnection.State.CONNECTING,
                    sessionStateEvent.event());
        }

        ConnectionMessageEvent<?> messageEvent = serverSink
                .take(ConnectionMessageEvent.class);
        assertEquals(serverConnection, messageEvent.connection());
        assertTrue(messageEvent.event() instanceof Operation.Request);
        assertEquals(connectRequest, ((Operation.Request) messageEvent.event()));

        Operation.Response connectResponse = OpCreateSessionAction.InvalidResponse
                .create();
        serverConnection.send(connectResponse).get();
        serverConnection.flush().get();
        clientConnection.read();

        for (EventfulSink sink : sinks) {
            ConnectionSessionStateEvent sessionStateEvent = sink
                    .take(ConnectionSessionStateEvent.class);
            assertEquals(SessionConnection.State.ERROR,
                    sessionStateEvent.event());
        }

        messageEvent = clientSink.take(ConnectionMessageEvent.class);
        assertEquals(clientConnection, messageEvent.connection());
        assertTrue(messageEvent.event() instanceof Operation.Result);
        Operation.Result result = (Operation.Result) messageEvent.event();
        assertEquals(connectRequest, result.request());
        assertEquals(connectResponse, result.response());

        clients.stopAndWait();
        server.stopAndWait();

        for (EventfulSink sink : sinks) {
            ConnectionStateEvent connectionStateEvent = sink
                    .take(ConnectionStateEvent.class);
            while (connectionStateEvent.event() != Connection.State.CONNECTION_CLOSED) {
                connectionStateEvent = sink.take(ConnectionStateEvent.class);
            }
            assertTrue(sink.toString(), sink.isEmpty());
        }

        assertEquals(Connection.State.CONNECTION_CLOSED,
                clientConnection.state());
        assertEquals(Connection.State.CONNECTION_CLOSED,
                serverConnection.state());
    }
}
