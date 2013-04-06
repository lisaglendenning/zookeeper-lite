package org.apache.zookeeper.netty.server;

import java.util.List;

import org.apache.zookeeper.server.ServerConnectionGroup;
import org.apache.zookeeper.server.ServerMain;
import org.apache.zookeeper.util.ServiceMonitor;

import com.google.inject.Module;
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
        bind(ServerConnection.Factory.class).in(Singleton.class);
    }
    
    @Provides @Singleton
    protected ServerConnectionGroup getServerConnectionGroup(ChannelServerConnectionGroup group, ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }

    @Override
    protected List<Module> modules() {
        List<Module> modules = super.modules();
        modules.add(NioServerBootstrapFactory.ServerBootstrapModule.get());
        return modules;
    }
}
