package edu.uw.zookeeper.server;


import java.net.SocketAddress;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.ServerExecutor;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.event.NewConnectionEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message.ClientMessage;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;
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
                ExpiringSessionManager manager = ExpiringSessionManager.newInstance(publisherFactory.get(), policy);
                ExpireSessionsTask expire = monitorsFactory.apply(ExpireSessionsTask.newInstance(manager, executors().asScheduledExecutorServiceFactory().get(), configuration()));
                
                final Singleton<? extends ServerExecutor> executor = Factories.holderOf(new ServerExecutor() {
                    @Override
                    public ListenableFuture<ServerMessage> submit(
                            ClientMessage request) {
                        System.out.println(request.toString());
                        return SettableFuture.create();
                    }});
                
                ParameterizedFactory<Connection, ServerCodecConnection> codecFactory = ServerCodecConnection.factory(publisherFactory());
                ParameterizedFactory<ServerCodecConnection, ServerProtocolConnection> protocolFactory =
                        new ParameterizedFactory<ServerCodecConnection, ServerProtocolConnection>() {
                            @Override
                            public ServerProtocolConnection get(
                                    ServerCodecConnection value) {
                                // TODO Auto-generated method stub
                                return ServerProtocolConnection.newInstance(value, executor.get(), executors.asListeningExecutorServiceFactory().get());
                            }
                };
                final ParameterizedFactory<Connection, ServerProtocolConnection> serverFactory = Factories.linkParameterized(codecFactory, protocolFactory);
                
                connections.register(new Object() {
                    @Subscribe
                    public void handle(NewConnectionEvent event) {
                        serverFactory.get(event.connection());
                    }
                });
                
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
