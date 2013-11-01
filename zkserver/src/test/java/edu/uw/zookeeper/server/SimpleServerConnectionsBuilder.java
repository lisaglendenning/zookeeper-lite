package edu.uw.zookeeper.server;

import java.net.InetSocketAddress;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;

public class SimpleServerConnectionsBuilder extends ServerConnectionsHandler.Builder {

    public static SimpleServerConnectionsBuilder defaults(
            IntraVmNetModule net) {
        ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) net.factory().addresses().get());
        return defaults(address, net);
    }
    
    public static SimpleServerConnectionsBuilder defaults(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return new SimpleServerConnectionsBuilder(connectionBuilder(address, serverModule), 
                null, null);
    }
    
    public static ServerConnectionFactoryBuilder connectionBuilder(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return ServerConnectionFactoryBuilder.defaults()
                .setServerModule(serverModule)
                .setAddress(address);
    }
    
    public SimpleServerConnectionsBuilder(
            ServerConnectionFactoryBuilder connectionBuilder,
            TimeValue timeOut, 
            ServerExecutor<?> serverExecutor) {
        super(connectionBuilder, timeOut, serverExecutor);
    }

    @Override
    public SimpleServerConnectionsBuilder setRuntimeModule(RuntimeModule runtime) {
        return (SimpleServerConnectionsBuilder) super.setRuntimeModule(runtime);
    }
    
    @Override
    public SimpleServerConnectionsBuilder setTimeOut(TimeValue timeOut) {
        return (SimpleServerConnectionsBuilder) super.setTimeOut(timeOut);
    }

    @Override
    public SimpleServerConnectionsBuilder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
        return (SimpleServerConnectionsBuilder) super.setConnectionBuilder(connectionBuilder);
    }

    @Override
    public SimpleServerConnectionsBuilder setServerExecutor(ServerExecutor<?> serverExecutor) {
        return (SimpleServerConnectionsBuilder) super.setServerExecutor(serverExecutor);
    }

    @Override
    public SimpleServerConnectionsBuilder setDefaults() {
        return (SimpleServerConnectionsBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleServerConnectionsBuilder newInstance(
            ServerConnectionFactoryBuilder connectionBuilder,
            TimeValue timeOut,
            ServerExecutor<?> serverExecutor) {
        return new SimpleServerConnectionsBuilder(connectionBuilder, timeOut, serverExecutor);
    }
    
    @Override
    protected TimeValue getDefaultTimeOut() {
        return TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT);
    }
}
