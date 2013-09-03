package edu.uw.zookeeper.server;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleServerBuilder extends ServerBuilder {

    public static SimpleServerBuilder defaults(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return new SimpleServerBuilder(connectionBuilder(address, serverModule), null, null, null);
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
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors) {
        super(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors);
    }

    @Override
    public SimpleServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return new SimpleServerBuilder(connectionBuilder.setRuntimeModule(runtime), serverConnectionFactory, getServerTaskExecutor(), connectionExecutors);
    }

    @Override
    public SimpleServerBuilder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
        return new SimpleServerBuilder(connectionBuilder, serverConnectionFactory, getServerTaskExecutor(), connectionExecutors);
    }

    @Override
    public SimpleServerBuilder setServerConnectionFactory(
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory) {
        return new SimpleServerBuilder(connectionBuilder, serverConnectionFactory, getServerTaskExecutor(), connectionExecutors);
    }

    @Override
    public SimpleServerBuilder setServerTaskExecutor(ServerTaskExecutor serverTaskExecutor) {
        return new SimpleServerBuilder(connectionBuilder, serverConnectionFactory, (SimpleServerExecutor) serverTaskExecutor, connectionExecutors);
    }

    @Override
    public SimpleServerBuilder setConnectionExecutors(ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors) {
        return new SimpleServerBuilder(connectionBuilder, serverConnectionFactory, getServerTaskExecutor(), connectionExecutors);
    }
    
    @Override
    public SimpleServerExecutor getServerTaskExecutor() {
        return (SimpleServerExecutor) serverTaskExecutor;
    }
    
    @Override
    public SimpleServerBuilder setDefaults() {
        return (SimpleServerBuilder) super.setDefaults();
    }

    @Override
    protected SimpleServerExecutor getDefaultServerTaskExecutor() {
        return SimpleServerExecutor.newInstance();
    }
}
