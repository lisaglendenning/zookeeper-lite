package edu.uw.zookeeper.protocol.client;


import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ScheduledExecutorService;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;

public class ClientConnectionFactoryBuilder implements ZooKeeperApplication.RuntimeBuilder<ClientConnectionFactory<? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>>, ClientConnectionFactoryBuilder> {

    public static ClientConnectionFactoryBuilder defaults() {
        return new ClientConnectionFactoryBuilder(null, null, null, null, null);
    }
    
    protected final RuntimeModule runtime;
    protected final NetClientModule clientModule;
    protected final TimeValue timeOut;
    protected final Factory<? extends ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession>> codecFactory;
    protected final ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>, ? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> connectionFactory;
    
    public ClientConnectionFactoryBuilder(
            RuntimeModule runtime,
            NetClientModule clientModule,
            TimeValue timeOut,
            Factory<? extends ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession>> codecFactory,
            ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>, ? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> connectionFactory) {
        super();
        this.runtime = runtime;
        this.clientModule = clientModule;
        this.timeOut = timeOut;
        this.codecFactory = codecFactory;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Override
    public ClientConnectionFactoryBuilder setRuntimeModule(RuntimeModule runtime) {
        if (this.runtime == runtime) {
            return this;
        } else {
            return newInstance(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }
    
    public NetClientModule getClientModule() {
        return clientModule;
    }

    public ClientConnectionFactoryBuilder setClientModule(NetClientModule clientModule) {
        if (this.clientModule == clientModule) {
            return this;
        } else {
            return newInstance(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public TimeValue getTimeOut() {
        return timeOut;
    }

    public ClientConnectionFactoryBuilder setTimeOut(TimeValue timeOut) {
        if (this.timeOut == timeOut) {
            return this;
        } else {
            return newInstance(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public Factory<? extends ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession>> getCodecFactory() {
        return codecFactory;
    }

    public ClientConnectionFactoryBuilder setCodecFactory(
            Factory<? extends ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession>> codecFactory) {
        if (this.codecFactory == codecFactory) {
            return this;
        } else {
            return newInstance(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>, ? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> getConnectionFactory() {
        return connectionFactory;
    }

    public ClientConnectionFactoryBuilder setConnectionFactory(
            ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>, ? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> connectionFactory) {
        return newInstance(runtime, clientModule, timeOut, codecFactory, connectionFactory);
    }

    @Override
    public ClientConnectionFactoryBuilder setDefaults() {
        checkState(runtime != null);
    
        if (clientModule == null) {
            return setClientModule(getDefaultClientModule()).setDefaults();
        }
        if (timeOut == null) {
            return setTimeOut(getDefaultTimeOut()).setDefaults();
        }
        if (codecFactory == null) {
            return setCodecFactory(getDefaultCodecFactory()).setDefaults();
        }
        if (connectionFactory == null) {
            return setConnectionFactory(getDefaultConnectionFactory()).setDefaults();
        }
        return this;
    }

    @Override
    public ClientConnectionFactory<? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession, ?,?>> build() {
        return setDefaults().getDefaultClientConnectionFactory();
    }

    protected ClientConnectionFactoryBuilder newInstance(
            RuntimeModule runtime,
            NetClientModule clientModule,
            TimeValue timeOut,
            Factory<? extends ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession>> codecFactory,
            ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>, ? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> connectionFactory) {
        return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
    }

    protected TimeValue getDefaultTimeOut() {
        return ZooKeeperApplication.ConfigurableTimeout.get(runtime.getConfiguration());
    }
    
    protected NetClientModule getDefaultClientModule() {
        return NettyClientModule.newInstance(runtime);
    }
    
    protected Factory<? extends ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession>> getDefaultCodecFactory() {
        return new Factory<ClientProtocolCodec>(){
            @Override
            public ClientProtocolCodec get() {
                return ClientProtocolCodec.defaults();
            }
        };
    }
    
    protected ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>, ? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> getDefaultConnectionFactory() {
        return PingingClient.<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>, CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession, Message.ClientSession, Message.ServerSession>,?>>factory(
                getTimeOut(), 
                getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
    }
    
    protected ClientConnectionFactory<? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> getDefaultClientConnectionFactory() {
        ClientConnectionFactory<? extends ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>> clientConnections = 
                getClientModule().getClientConnectionFactory(
                            getCodecFactory(), 
                            getConnectionFactory()).get();
        return clientConnections;
    }
}
