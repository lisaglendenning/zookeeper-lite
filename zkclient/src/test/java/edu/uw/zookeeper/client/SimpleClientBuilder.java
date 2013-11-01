package edu.uw.zookeeper.client;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;

public class SimpleClientBuilder extends ConnectionClientExecutorService.Builder {
    
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
                .setConnectionFactory(
                        new ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession,Message.ClientSession,Message.ServerSession>,?>, ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>>() {
                            @Override
                            public ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?> get(CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession,Message.ClientSession,Message.ServerSession>,?> value) {
                                return ClientProtocolConnection.newInstance(value);
                            }
                        });
    }
    
    protected final ServerInetAddressView serverAddress;

    protected SimpleClientBuilder(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
            ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor,
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
            ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory) {
        return (SimpleClientBuilder) super.setClientConnectionFactory(clientConnectionFactory);
    }

    @Override
    public SimpleClientBuilder setConnectionClientExecutor(
            ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor) {
        return (SimpleClientBuilder) super.setConnectionClientExecutor(clientExecutor);
    }

    @Override
    public SimpleClientBuilder setDefaults() {
        return (SimpleClientBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleClientBuilder newInstance(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
            ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor,
            RuntimeModule runtime) {
        return newInstance(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }

    protected SimpleClientBuilder newInstance(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
            ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor,
            RuntimeModule runtime) {
        return new SimpleClientBuilder(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }
    
    @Override    
    protected EnsembleView<ServerInetAddressView> getDefaultEnsemble() {
        return EnsembleView.of(serverAddress);
    }
}
