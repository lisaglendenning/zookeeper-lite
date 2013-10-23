package edu.uw.zookeeper.server;

import java.net.InetSocketAddress;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;

public class SimpleServerBuilder extends ServerConnectionsHandler.Builder {

    public static SimpleServerBuilder defaults(
            IntraVmNetModule net) {
        ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) net.factory().addresses().get());
        return defaults(address, net);
    }
    
    public static SimpleServerBuilder defaults(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return new SimpleServerBuilder(connectionBuilder(address, serverModule), 
                null, null);
    }
    
    public static ServerConnectionFactoryBuilder connectionBuilder(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return ServerConnectionFactoryBuilder.defaults()
                .setServerModule(serverModule)
                .setAddress(address);
    }
    
    public SimpleServerBuilder(
            ServerConnectionFactoryBuilder connectionBuilder,
            TimeValue timeOut, 
            ServerExecutor serverExecutor) {
        super(connectionBuilder, timeOut, serverExecutor);
    }

    @Override
    public SimpleServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return (SimpleServerBuilder) super.setRuntimeModule(runtime);
    }
    
    @Override
    public SimpleServerBuilder setTimeOut(TimeValue timeOut) {
        return (SimpleServerBuilder) super.setTimeOut(timeOut);
    }

    @Override
    public SimpleServerBuilder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
        return (SimpleServerBuilder) super.setConnectionBuilder(connectionBuilder);
    }

    @Override
    public SimpleServerBuilder setServerExecutor(ServerExecutor serverExecutor) {
        return (SimpleServerBuilder) super.setServerExecutor(serverExecutor);
    }

    @Override
    public SimpleServerBuilder setDefaults() {
        return (SimpleServerBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleServerBuilder newInstance(
            ServerConnectionFactoryBuilder connectionBuilder,
            TimeValue timeOut,
            ServerExecutor serverExecutor) {
        return new SimpleServerBuilder(connectionBuilder, timeOut, serverExecutor);
    }
    
    @Override
    protected TimeValue getDefaultTimeOut() {
        return TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT);
    }
}
