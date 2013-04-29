package edu.uw.zookeeper.server;


import java.net.SocketAddress;

import com.google.common.util.concurrent.Service;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.AbstractMain.MonitorServiceFactory;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
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

                ParameterizedFactory<? extends SocketAddress, ? extends ChannelServerConnectionFactory> connections = connections();
                return null;
            }
        });
    }

    @Override
    protected Application application() {
        return application.get();
    }
    
    protected abstract ParameterizedFactory<? extends SocketAddress, ? extends ChannelServerConnectionFactory> connections();
}
