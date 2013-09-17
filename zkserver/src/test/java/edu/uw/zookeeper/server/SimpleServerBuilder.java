package edu.uw.zookeeper.server;

import java.net.InetSocketAddress;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleServerBuilder extends ServerConnectionExecutorsService.Builder {

    public static SimpleServerBuilder defaults(
            IntraVmNetModule net) {
        ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) net.factory().addresses().get());
        return defaults(address, net);
    }
    
    public static SimpleServerBuilder defaults(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return new SimpleServerBuilder(
                connectionBuilder(address, serverModule), null, null, null, null);
    }
    
    public static ServerConnectionFactoryBuilder connectionBuilder(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return ServerConnectionFactoryBuilder.defaults()
                .setServerModule(serverModule)
                .setAddress(address)
                .setTimeOut(TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT));
    }
    
    protected SimpleServerBuilder(
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            SimpleServerExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        super(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }

    @Override
    public SimpleServerExecutor getServerTaskExecutor() {
        return (SimpleServerExecutor) serverTaskExecutor;
    }

    @Override
    public SimpleServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return (SimpleServerBuilder) super.setRuntimeModule(runtime);
    }

    @Override
    public SimpleServerBuilder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
        return (SimpleServerBuilder) super.setConnectionBuilder(connectionBuilder);
    }

    @Override
    public SimpleServerBuilder setServerConnectionFactory(
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory) {
        return (SimpleServerBuilder) super.setServerConnectionFactory(serverConnectionFactory);
    }

    @Override
    public SimpleServerBuilder setServerTaskExecutor(ServerTaskExecutor serverTaskExecutor) {
        return (SimpleServerBuilder) super.setServerTaskExecutor((SimpleServerExecutor) serverTaskExecutor);
    }

    @Override
    public SimpleServerBuilder setConnectionExecutors(ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors) {
        return (SimpleServerBuilder) super.setConnectionExecutors(connectionExecutors);
    }
    
    @Override
    public SimpleServerBuilder setDefaults() {
        return (SimpleServerBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleServerBuilder newInstance(
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        return new SimpleServerBuilder(connectionBuilder, serverConnectionFactory, (SimpleServerExecutor) serverTaskExecutor, connectionExecutors, runtime);
    }
    
    @Override
    protected SimpleServerExecutor getDefaultServerTaskExecutor() {
        return SimpleServerExecutor.newInstance();
    }
}
