package edu.uw.zookeeper.netty.server;

import java.util.List;


import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.server.ServerConnectionGroup;
import edu.uw.zookeeper.server.ServerMain;
import edu.uw.zookeeper.util.ServiceMonitor;

public class NettyServerMain extends ServerMain {

    public static void main(String[] args) throws Exception {
        NettyServerMain main = get();
        main.apply(args);
    }

    public static NettyServerMain get() {
        return new NettyServerMain();
    }

    protected NettyServerMain() {
    }

    @Override
    protected void configure() {
        super.configure();
        bind(ConfigurableChannelServerConnectionGroup.class)
                .in(Singleton.class);
        bind(ServerConnection.Factory.class).in(Singleton.class);
    }

    @Provides
    @Singleton
    protected ServerConnectionGroup getServerConnectionGroup(
            ConfigurableChannelServerConnectionGroup group,
            ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }

    @Override
    protected List<Module> modules() {
        List<Module> modules = super.modules();
        modules.add(NioServerBootstrapFactory.get());
        return modules;
    }
}
