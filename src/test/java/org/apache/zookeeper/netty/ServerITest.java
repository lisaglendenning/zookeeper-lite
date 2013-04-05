package org.apache.zookeeper.netty;

import static org.junit.Assert.*;

import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.EventSink;
import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.client.Client;
import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.client.ClientSessionConnection;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.event.ConnectionEvent;
import org.apache.zookeeper.event.ConnectionMessageEvent;
import org.apache.zookeeper.event.ConnectionSessionStateEvent;
import org.apache.zookeeper.event.ConnectionStateEvent;
import org.apache.zookeeper.event.SessionConnectionStateEvent;
import org.apache.zookeeper.event.SessionEvent;
import org.apache.zookeeper.event.SessionResponseEvent;
import org.apache.zookeeper.event.SessionStateEvent;
import org.apache.zookeeper.netty.client.ChannelClientConnectionGroup;
import org.apache.zookeeper.netty.client.ClientConnection;
import org.apache.zookeeper.netty.server.ChannelServerConnectionGroup;
import org.apache.zookeeper.netty.server.ServerConnection;
import org.apache.zookeeper.server.DefaultSessionParametersPolicy;
import org.apache.zookeeper.server.ExpiringSessionManager;
import org.apache.zookeeper.server.RequestExecutor;
import org.apache.zookeeper.server.Server;
import org.apache.zookeeper.server.ServerConnectionGroup;
import org.apache.zookeeper.server.SessionManager;
import org.apache.zookeeper.server.SessionParametersPolicy;
import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulEventBus;
import org.apache.zookeeper.util.ServiceMonitor;
import org.apache.zookeeper.util.SettableConfiguration;
import org.apache.zookeeper.util.SimpleArguments;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class ServerITest {

    @Rule
    public Timeout globalTimeout = new Timeout(10000); 

    protected final Logger logger = LoggerFactory.getLogger(ServerITest.class);

    public static class Module extends LocalModule {

        public static Injector injector = null;
        
        public static void createInjector() {
            injector = Guice.createInjector(
                    Module.get());
        }

        public static Module get() {
            return new Module();
        }
        
        @Override
        protected void configure() {
            // channels
            super.configure();
            
            // utilities
            bind(Arguments.class).to(SimpleArguments.class).in(Singleton.class);
            bind(Configuration.class).to(SettableConfiguration.class).in(Singleton.class);
            bind(Eventful.class).to(EventfulEventBus.class);
            bind(ServiceMonitor.class).in(Singleton.class);
            
            // server
            bind(SessionParametersPolicy.class).to(DefaultSessionParametersPolicy.class).in(Singleton.class);
            bind(ExpiringSessionManager.class).in(Singleton.class);
            bind(SessionManager.class).to(ExpiringSessionManager.class).in(Singleton.class);
            bind(ChannelServerConnectionGroup.class).in(Singleton.class);
            bind(RequestExecutorService.Factory.class).to(RequestExecutor.Factory.class).in(Singleton.class);
            bind(Server.class).in(Singleton.class);

            // client
            bind(ChannelClientConnectionGroup.class).in(Singleton.class);
            bind(ClientSessionConnection.Factory.class).in(Singleton.class);
            bind(Client.Factory.class).in(Singleton.class);
            bind(Client.class).toProvider(Client.Factory.class);
        }

        @Provides @Singleton
        public Zxid zxid() {
            return Zxid.create();
        }

        @Provides @Singleton
        protected ServerConnectionGroup getServerConnectionGroup(ChannelServerConnectionGroup group, ServiceMonitor monitor) {
            monitor.add(group);
            return group;
        }

        @Provides @Singleton
        protected ServerConnection.Factory getServerConnectionFactory(Provider<Eventful> eventfulFactory, Zxid zxid) {
            return ServerConnection.Factory.get(eventfulFactory, zxid);
        }

        @Provides @Singleton
        public Xid xid() {
            return Xid.create();
        }
        
        @Provides @Singleton
        protected ClientConnectionGroup getClientConnectionGroup(ChannelClientConnectionGroup group, ServiceMonitor monitor) {
            monitor.add(group);
            return group;
        }

        @Provides @Singleton
        protected ClientConnection.Factory getClientConnectionFactory(Provider<Eventful> eventfulFactory, Xid xid) {
            return ClientConnection.Factory.get(eventfulFactory, xid);
        }
        
        @Provides @Singleton
        public ScheduledExecutorService scheduledExecutorService() {
            return Executors.newSingleThreadScheduledExecutor();
        }

        @Provides @Singleton
        public Executor getExecutor(ExecutorService executor) {
            return executor;
        }

        @Provides @Singleton
        public ExecutorService getExecutorService() {
            return MoreExecutors.sameThreadExecutor();
        }

        @Provides @Singleton
        public ListeningExecutorService getExecutorService(ExecutorService executor) {
            return MoreExecutors.listeningDecorator(executor);
        }
    }

    public static class EventfulSink extends EventSink {
    	@Subscribe @AllowConcurrentEvents
    	public void handleEvent(Connection event) throws InterruptedException {
    		put(Connection.class, event);
    	}

    	@Subscribe @AllowConcurrentEvents
    	public void handleEvent(SessionEvent event) throws InterruptedException {
    		put(event);
    	}

    	@Subscribe @AllowConcurrentEvents
    	public void handleEvent(ConnectionEvent event) throws InterruptedException {
    		put(event);
    	}
    }

    @BeforeClass
    public static void startup() {
        Module.createInjector();
        Injector injector = Module.injector;
        injector.getInstance(Server.class);
        injector.getInstance(ServerConnectionGroup.class);
        injector.getInstance(ClientConnectionGroup.class);
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAndWait();
    }

    @AfterClass
    public static void shutdown() {
        Injector injector = Module.injector;
    	ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.stopAndWait();
    }
    
    @Test
    public void testClientSessionConnectClose() throws Exception {
        Injector injector = Module.injector;
        
        EventfulSink[] sinks = { new EventfulSink(), new EventfulSink() };
        EventfulSink clientEventSink = sinks[0];
        EventfulSink serverEventSink = sinks[1];
        
        ExpiringSessionManager sessions = injector.getInstance(ExpiringSessionManager.class);
        sessions.register(serverEventSink);
        ServerConnectionGroup serverConnections = injector.getInstance(ServerConnectionGroup.class);
        SocketAddress serverAddress = serverConnections.localAddress();
        serverConnections.register(serverEventSink);

        ClientConnectionGroup clientConnections = injector.getInstance(ClientConnectionGroup.class);
        clientConnections.register(clientEventSink);
        Connection clientConnection = clientConnections.connect(serverAddress).get();
        assertSame(clientConnection, clientEventSink.take(Connection.class));
        assertSame(clientConnection, Iterables.getOnlyElement(clientConnections));
        clientConnection.register(clientEventSink);

        // connect
        ClientSessionConnection clientSession = injector.getInstance(ClientSessionConnection.Factory.class).get(clientConnection);
        clientSession.register(clientEventSink);
        assertEquals(SessionConnection.State.ANONYMOUS, clientSession.state());
        assertFalse(clientSession.session().initialized());
        Operation.Result result = clientSession.connect().get();
        assertEquals(Operation.CREATE_SESSION, result.operation());
        assertFalse(result instanceof Operation.Error);
        
        SessionConnection.State sessionConnectionState = clientEventSink.take(ConnectionSessionStateEvent.class).event();
        assertEquals(SessionConnection.State.CONNECTING, sessionConnectionState);
        sessionConnectionState = clientEventSink.take(ConnectionSessionStateEvent.class).event();
        assertEquals(SessionConnection.State.CONNECTED, sessionConnectionState);
        
        ConnectionMessageEvent<?> messageEvent = clientEventSink.take(ConnectionMessageEvent.class);
        assertEquals(result, messageEvent.event());
        
        Session session = clientSession.session();
        assertTrue(session.initialized());
        SessionStateEvent sessionStateEvent = serverEventSink.take(SessionStateEvent.class);
        assertEquals(Session.State.SESSION_OPENED, sessionStateEvent.event());
        assertEquals(session, sessionStateEvent.session());
        assertEquals(session, Iterables.getOnlyElement(sessions));
        SessionResponseEvent responseEvent = clientEventSink.take(SessionResponseEvent.class);
        assertEquals(session, responseEvent.session());
        assertEquals(result, responseEvent.event());
        
        sessionConnectionState = clientEventSink.take(SessionConnectionStateEvent.class).event();
        assertEquals(SessionConnection.State.CONNECTED, sessionConnectionState);
        assertEquals(sessionConnectionState, clientSession.state());
        
        Connection serverConnection = serverEventSink.take(Connection.class);
        assertSame(serverConnection, Iterables.getOnlyElement(serverConnections));
        serverConnection.register(serverEventSink);

        // disconnect
        result = clientSession.disconnect().get();
        assertEquals(Operation.CLOSE_SESSION, result.operation());
        sessionConnectionState = clientEventSink.take(SessionConnectionStateEvent.class).event();
        assertEquals(SessionConnection.State.DISCONNECTING, sessionConnectionState);
        sessionConnectionState = clientEventSink.take(SessionConnectionStateEvent.class).event();
        assertEquals(SessionConnection.State.DISCONNECTED, sessionConnectionState);
        assertEquals(sessionConnectionState, clientSession.state());
        responseEvent = clientEventSink.take(SessionResponseEvent.class);
        assertEquals(session, responseEvent.session());
        assertEquals(result, responseEvent.event());

        messageEvent = serverEventSink.take(ConnectionMessageEvent.class);
        assertEquals(result.request(), messageEvent.event());
        
        // session close should close the connections
        for (EventfulSink sink: sinks) {
        	SessionConnection.State[] expectedStates = { 
        			SessionConnection.State.DISCONNECTING,
        			SessionConnection.State.DISCONNECTED
        	};
        	for (SessionConnection.State expectedState: expectedStates) {
        		assertEquals(expectedState, sink.take(ConnectionSessionStateEvent.class).event());
        	}
        }
        
        messageEvent = clientEventSink.take(ConnectionMessageEvent.class);
        assertEquals(result, messageEvent.event());
        
        sessionStateEvent = serverEventSink.take(SessionStateEvent.class);
        assertEquals(session, sessionStateEvent.session());
        assertEquals(Session.State.SESSION_CLOSED, sessionStateEvent.event());
        assertEquals(0, Iterables.size(sessions));
        
        for (EventfulSink sink: sinks) {
        	Connection.State[] expectedStates = { 
        			Connection.State.CONNECTION_CLOSING,
        			Connection.State.CONNECTION_CLOSED
        	};
        	for (Connection.State expectedState: expectedStates) {
        		assertEquals(expectedState, sink.take(ConnectionStateEvent.class).event());
        	}
        }
        
        assertTrue(serverEventSink.toString(), serverEventSink.isEmpty());
        assertTrue(clientEventSink.toString(), clientEventSink.isEmpty());
    }
    
    @Test
    public void testClientSessionExpire() throws Exception {
        Injector injector = Module.injector;

        EventfulSink[] sinks = { new EventfulSink(), new EventfulSink() };
        EventfulSink clientEventSink = sinks[0];
        EventfulSink serverEventSink = sinks[1];
        
        ExpiringSessionManager sessions = injector.getInstance(ExpiringSessionManager.class);
        sessions.register(serverEventSink);
        ServerConnectionGroup serverConnections = injector.getInstance(ServerConnectionGroup.class);
        SocketAddress serverAddress = serverConnections.localAddress();
        serverConnections.register(serverEventSink);

        ClientConnectionGroup clientConnections = injector.getInstance(ClientConnectionGroup.class);
        clientConnections.register(clientEventSink);
        Connection clientConnection = clientConnections.connect(serverAddress).get();
        clientConnection.register(clientEventSink);
        
        ClientSessionConnection clientSession = injector.getInstance(ClientSessionConnection.Factory.class).get(clientConnection);
        clientSession.register(clientEventSink);
        clientSession.connect().get();

        SessionStateEvent sessionStateEvent = serverEventSink.take(SessionStateEvent.class);
        assertEquals(clientSession.session(), sessionStateEvent.session());
        assertEquals(Session.State.SESSION_OPENED, sessionStateEvent.event());

        Connection serverConnection = serverEventSink.take(Connection.class);
        serverConnection.register(serverEventSink);
        
        // expire!
        sessions.expire(clientSession.session().id());
        sessionStateEvent = serverEventSink.take(SessionStateEvent.class);
        assertEquals(clientSession.session(), sessionStateEvent.session());
        assertEquals(Session.State.SESSION_EXPIRED, sessionStateEvent.event());
        sessionStateEvent = serverEventSink.take(SessionStateEvent.class);
        assertEquals(clientSession.session(), sessionStateEvent.session());
        assertEquals(Session.State.SESSION_CLOSED, sessionStateEvent.event());
        
        // the server should now close the connection
        for (EventfulSink sink: sinks) {
        	Connection.State connectionState = sink.take(ConnectionStateEvent.class).event();
        	while (connectionState != Connection.State.CONNECTION_CLOSED) {
        		connectionState = sink.take(ConnectionStateEvent.class).event();
        	}
        }
    }
}
