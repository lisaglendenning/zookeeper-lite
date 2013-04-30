package edu.uw.zookeeper.server;


import java.net.SocketAddress;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.ServerExecutor;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.event.NewConnectionEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message.ClientMessage;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.Operation.SessionReply;
import edu.uw.zookeeper.protocol.Operation.SessionRequest;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;

public abstract class ServerMain extends AbstractMain {

    protected final Singleton<Application> application;
    
    protected ServerMain(Configuration configuration) {
        super(configuration);
        this.application = Factories.lazyFrom(new Factory<Application>() {
            @Override
            public Application get() {
                ServiceMonitor monitor = serviceMonitor();
                MonitorServiceFactory monitorsFactory = monitors(monitor);

                ServerView.Address<?> address = ConfigurableServerAddressViewFactory.newInstance().get(configuration());
                ServerConnectionFactory connections = monitorsFactory.apply(connectionFactory().get(address.get()));
                
                SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(configuration());
                ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(publisherFactory.get(), policy);
                ExpireSessionsTask expire = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, executors().asScheduledExecutorServiceFactory().get(), configuration()));
                
                ZxidIncrementer zxids = ZxidIncrementer.newInstance();
                final OpCreateSessionProcessor processor = OpCreateSessionProcessor.newInstance(sessions, zxids);
                final Singleton<? extends ServerExecutor> anonymousExecutor = Factories.holderOf(new ServerExecutor() {
                    @Override
                    public ListenableFuture<ServerMessage> submit(
                            ClientMessage message) {
                        System.out.println(message.toString());
                        SettableFuture<ServerMessage> future = SettableFuture.create();
                        if (message instanceof OpCreateSession.Request) {
                            try {
                                future.set(processor.apply((OpCreateSession.Request)message));
                            } catch (Exception e) {
                                future.setException(e);
                            }
                        }
                        return future;
                    }});
                
                final ParameterizedFactory<Long, SessionRequestExecutor> sessionExecutors = new ParameterizedFactory<Long, SessionRequestExecutor>() {
                    @Override
                    public SessionRequestExecutor get(final Long value) {
                        return new SessionRequestExecutor() {

                            @Override
                            public ListenableFuture<SessionReply> submit(
                                    SessionRequest request) {
                                System.out.printf("0x%s: %s%n", Long.toHexString(value), request);
                                return SettableFuture.create();
                            }

                            @Override
                            public void register(Object object) {
                            }

                            @Override
                            public void unregister(Object object) {
                            }
                            
                        };
                    }};
                
                ParameterizedFactory<Connection, ServerCodecConnection> codecFactory = ServerCodecConnection.factory(publisherFactory());
                ParameterizedFactory<ServerCodecConnection, ServerProtocolConnection> protocolFactory =
                        new ParameterizedFactory<ServerCodecConnection, ServerProtocolConnection>() {
                            @Override
                            public ServerProtocolConnection get(
                                    ServerCodecConnection value) {
                                // TODO Auto-generated method stub
                                return ServerProtocolConnection.newInstance(value, anonymousExecutor.get(), sessionExecutors, executors.asListeningExecutorServiceFactory().get());
                            }
                };
                final ParameterizedFactory<Connection, ServerProtocolConnection> serverFactory = Factories.linkParameterized(codecFactory, protocolFactory);
                
                connections.register(new Object() {
                    @Subscribe
                    public void handle(NewConnectionEvent event) {
                        serverFactory.get(event.connection());
                    }
                });
                
                // pre-create executor
                executors().asListeningExecutorServiceFactory().get();
                
                return ServerMain.super.application();
            }
        });
    }

    @Override
    protected Application application() {
        return application.get();
    }
    
    protected abstract ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory> connectionFactory();
}
