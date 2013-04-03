package org.apache.zookeeper.netty;

import static org.junit.Assert.*;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.client.ClientSession;
import org.apache.zookeeper.netty.protocol.client.ChannelClientConnectionGroup;
import org.apache.zookeeper.netty.protocol.client.ClientConnection;
import org.apache.zookeeper.netty.protocol.server.ChannelServerConnectionGroup;
import org.apache.zookeeper.netty.protocol.server.ServerConnection;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.server.DefaultSessionParametersPolicy;
import org.apache.zookeeper.server.ExpiringSessionManager;
import org.apache.zookeeper.server.RequestExecutorFactory;
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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
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
            //bind(ExpireSessionsTask.class).in(Singleton.class); // todo
            bind(SessionManager.class).to(ExpiringSessionManager.class).in(Singleton.class);
            bind(ChannelServerConnectionGroup.class).in(Singleton.class);
            bind(RequestExecutorService.Factory.class).to(RequestExecutorFactory.class).in(Singleton.class);
            bind(Server.class).in(Singleton.class);

            // client
            bind(ChannelClientConnectionGroup.class).in(Singleton.class);
            //bind(PingSessionsTask.class).in(Singleton.class); // todo
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
        
        @Provides
        public ClientSession getClientSession(ClientSession.Factory factory) {
            return factory.get();
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

    @BeforeClass
    public static void createInjector() {
        Module.createInjector();
    }
    
    @Test
    public void test() throws Exception {
        Injector injector = Module.injector;
        Server server = injector.getInstance(Server.class);
        ExpiringSessionManager sessions = injector.getInstance(ExpiringSessionManager.class);
        ServerConnectionGroup serverConnections = injector.getInstance(ServerConnectionGroup.class);
        ClientConnectionGroup clientConnections = injector.getInstance(ClientConnectionGroup.class);
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAndWait();

        ClientSession clientSession = injector.getInstance(ClientSession.class);
        Connection clientConnection = clientConnections.connect(serverConnections.localAddress()).get();
        Operation.Result result = clientSession.connect(clientConnection).get();
        assertEquals(Operation.CREATE_SESSION, result.operation());
        Session session = Iterables.getOnlyElement(sessions);
        assertEquals(session, clientSession.session());
        assertEquals(Session.State.OPENED, clientSession.state());
        
        Connection serverConnection = Iterables.getOnlyElement(serverConnections);
        assertEquals(clientConnection, Iterables.getOnlyElement(clientConnections));
        
        result = clientSession.close().get();
        assertEquals(Operation.CLOSE_SESSION, result.operation());
        assertEquals(Session.State.CLOSED, clientSession.state());
        assertEquals(0, Iterables.size(sessions));
        
        
        monitor.stopAndWait();
    }
    
}
