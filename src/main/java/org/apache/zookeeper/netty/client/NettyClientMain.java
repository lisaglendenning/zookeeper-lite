package org.apache.zookeeper.netty.client;

import java.util.List;

import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.client.ClientMain;
import org.apache.zookeeper.util.ServiceMonitor;

import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class NettyClientMain extends ClientMain {

    public static void main(String[] args) throws Exception {
        NettyClientMain main = get();
        main.apply(args);
    }

    public static NettyClientMain get() {
        return new NettyClientMain();
    }

    protected NettyClientMain() {}
    
    @Override
    protected void configure() {
        super.configure();
        bind(ChannelClientConnectionGroup.class).in(Singleton.class);
        bind(ClientConnection.Factory.class).in(Singleton.class);
    }
    
    @Provides @Singleton
    protected ClientConnectionGroup getClientConnectionGroup(ChannelClientConnectionGroup group, ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }

    @Override
    protected List<Module> modules() {
        List<Module> modules = super.modules();
        modules.add(NioBootstrapFactory.BootstrapModule.get());
        return modules;
    }
}
