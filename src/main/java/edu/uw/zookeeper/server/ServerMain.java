package edu.uw.zookeeper.server;


import java.net.SocketAddress;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.net.ServerConnectionFactory;
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
