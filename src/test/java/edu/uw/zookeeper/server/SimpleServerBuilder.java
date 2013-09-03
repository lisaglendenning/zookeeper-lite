package edu.uw.zookeeper.server;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

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
        return new SimpleServerBuilder(null, connectionBuilder(address, serverModule), null, null);
    }
    
    public static ServerConnectionFactoryBuilder connectionBuilder(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        return ServerConnectionFactoryBuilder.defaults()
                .setServerModule(serverModule)
                .setAddress(address)
                .setTimeOut(TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT));
    }
    
    protected final ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors;
    
    protected SimpleServerBuilder(
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            SimpleServerExecutor serverTaskExecutor) {
        super(connectionBuilder, serverConnectionFactory, serverTaskExecutor);
        this.connectionExecutors = connectionExecutors;
    }

    public ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getConnectionExecutors() {
        return connectionExecutors;
    }

    public SimpleServerBuilder setConnectionExecutors(ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors) {
        return new SimpleServerBuilder(connectionExecutors, connectionBuilder, serverConnectionFactory, getServerTaskExecutor());
    }
    
    @Override
    public SimpleServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return new SimpleServerBuilder(connectionExecutors, connectionBuilder.setRuntimeModule(runtime), serverConnectionFactory, getServerTaskExecutor());
    }

    @Override
    public SimpleServerBuilder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
        return new SimpleServerBuilder(connectionExecutors, connectionBuilder, serverConnectionFactory, getServerTaskExecutor());
    }

    @Override
    public SimpleServerBuilder setServerConnectionFactory(
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory) {
        return new SimpleServerBuilder(connectionExecutors, connectionBuilder, serverConnectionFactory, getServerTaskExecutor());
    }

    @Override
    public SimpleServerBuilder setServerTaskExecutor(ServerTaskExecutor serverTaskExecutor) {
        return new SimpleServerBuilder(connectionExecutors, connectionBuilder, serverConnectionFactory, (SimpleServerExecutor) serverTaskExecutor);
    }

    @Override
    public SimpleServerExecutor getServerTaskExecutor() {
        return (SimpleServerExecutor) serverTaskExecutor;
    }
    
    @Override
    public SimpleServerBuilder setDefaults() {
        SimpleServerBuilder builder = (SimpleServerBuilder) super.setDefaults();
        if (builder == this) {
            if (connectionExecutors == null) {
                return setConnectionExecutors(getDefaultConnectionExecutorsService());
            }
        }
        return builder;
    }

    @Override
    protected SimpleServerExecutor getDefaultServerTaskExecutor() {
        return SimpleServerExecutor.newInstance();
    }
    
    @Override
    protected List<? extends Service> getServices() {
        return Lists.<Service>newArrayList(connectionExecutors);
    }
}
