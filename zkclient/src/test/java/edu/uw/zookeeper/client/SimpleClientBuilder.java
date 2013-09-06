package edu.uw.zookeeper.client;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

public class SimpleClientBuilder extends ClientBuilder {
    
    public static SimpleClientBuilder defaults(
            ServerInetAddressView serverAddress,
            NetClientModule clientModule) {
        return new SimpleClientBuilder(
                serverAddress, 
                connectionBuilder(clientModule), 
                null, 
                null, 
                null);
    }
    
    public static ClientConnectionFactoryBuilder connectionBuilder(
            NetClientModule clientModule) {
        return ClientConnectionFactoryBuilder.defaults()
                .setClientModule(clientModule)
                .setTimeOut(TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT))
                .setConnectionFactory(ProtocolCodecConnection.<Operation.Request,AssignXidCodec,Connection<Operation.Request>>factory());
    }
    
    protected final ServerInetAddressView serverAddress;

    protected SimpleClientBuilder(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        super(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        this.serverAddress = serverAddress;
    }

    public ServerInetAddressView getServerAddress() {
        return serverAddress;
    }
    
    public SimpleClientBuilder setServerAddress(ServerInetAddressView serverAddress) {
        if (this.serverAddress == serverAddress) {
            return this;
        } else {
            return newInstance(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        }
    }

    @Override
    public SimpleClientBuilder setRuntimeModule(RuntimeModule runtime) {
        return (SimpleClientBuilder) super.setRuntimeModule(runtime);
    }

    @Override
    public SimpleClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return (SimpleClientBuilder) super.setConnectionBuilder(connectionBuilder);
    }

    @Override
    public SimpleClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        return (SimpleClientBuilder) super.setClientConnectionFactory(clientConnectionFactory);
    }

    @Override
    public SimpleClientBuilder setClientConnectionExecutor(
            ClientConnectionExecutorService clientExecutor) {
        return (SimpleClientBuilder) super.setClientConnectionExecutor(clientExecutor);
    }

    @Override
    public SimpleClientBuilder setDefaults() {
        return (SimpleClientBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleClientBuilder newInstance(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        return newInstance(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }

    protected SimpleClientBuilder newInstance(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        return new SimpleClientBuilder(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }
    
    @Override    
    protected ClientConnectionExecutorService getDefaultClientConnectionExecutorService() {
        Factory<? extends ListenableFuture<? extends ClientConnectionExecutor<?>>> factory = 
                ServerViewFactory.newInstance(
                        clientConnectionFactory, 
                        serverAddress, 
                        getConnectionBuilder().getTimeOut(), 
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
        ClientConnectionExecutorService service =
                ClientConnectionExecutorService.newInstance(
                        factory);
        return service;
    }
}