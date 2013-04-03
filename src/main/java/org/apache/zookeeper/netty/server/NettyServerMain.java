package org.apache.zookeeper.netty.server;

import java.util.List;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.server.ServerConnectionGroup;
import org.apache.zookeeper.server.ServerMain;
import org.apache.zookeeper.netty.protocol.server.ChannelServerConnectionGroup;
import org.apache.zookeeper.netty.protocol.server.ServerConnection;
import org.apache.zookeeper.netty.server.NioServerBootstrapFactory.ServerBootstrapModule;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.ServiceMonitor;

import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class NettyServerMain extends ServerMain {

    public static void main(String[] args) throws Exception {
        NettyServerMain main = get();
        main.apply(args);
    }

    public static NettyServerMain get() {
        return new NettyServerMain();
    }

    protected NettyServerMain() {}
    
    @Override
    protected void configure() {
        super.configure();
        bind(ChannelServerConnectionGroup.class).in(Singleton.class);
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
    
    @Override
    protected List<Module> modules() {
        List<Module> modules = super.modules();
        modules.add(NioServerBootstrapFactory.ServerBootstrapModule.get());
        return modules;
    }
}