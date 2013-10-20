package edu.uw.zookeeper.client;


import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;

public class ClientConnectionFactoryBuilder implements ZooKeeperApplication.RuntimeBuilder<ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>>, ClientConnectionFactoryBuilder> {

    public static ClientConnectionFactoryBuilder defaults() {
        return new ClientConnectionFactoryBuilder();
    }
    
    protected final RuntimeModule runtime;
    protected final NetClientModule clientModule;
    protected final TimeValue timeOut;
    protected final ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>> codecFactory;
    protected final ParameterizedFactory<Pair<? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>, Connection<Message.ClientSession>>, ? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connectionFactory;
    
    public ClientConnectionFactoryBuilder() {
        this(null, null, null, null, null);
    }

    public ClientConnectionFactoryBuilder(
            RuntimeModule runtime,
            NetClientModule clientModule,
            TimeValue timeOut,
            ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>> codecFactory,
            ParameterizedFactory<Pair<? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>, Connection<Message.ClientSession>>, ? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connectionFactory) {
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

    public ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>> getCodecFactory() {
        return codecFactory;
    }

    public ClientConnectionFactoryBuilder setCodecFactory(
            ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>> codecFactory) {
        if (this.codecFactory == codecFactory) {
            return this;
        } else {
            return newInstance(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public ParameterizedFactory<? extends Pair<? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>, Connection<Message.ClientSession>>, ? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getConnectionFactory() {
        return connectionFactory;
    }

    public ClientConnectionFactoryBuilder setConnectionFactory(
            ParameterizedFactory<Pair<? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>, Connection<Message.ClientSession>>, ? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connectionFactory) {
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
    public ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> build() {
        return setDefaults().getDefaultClientConnectionFactory();
    }

    protected ClientConnectionFactoryBuilder newInstance(
            RuntimeModule runtime,
            NetClientModule clientModule,
            TimeValue timeOut,
            ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>> codecFactory,
            ParameterizedFactory<Pair<? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>, Connection<Message.ClientSession>>, ? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> connectionFactory) {
        return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
    }

    protected TimeValue getDefaultTimeOut() {
        return ZooKeeperApplication.ConfigurableTimeout.get(runtime.getConfiguration());
    }
    
    protected NetClientModule getDefaultClientModule() {
        return NettyClientModule.newInstance(runtime);
    }
    
    protected ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>> getDefaultCodecFactory() {
        return ClientProtocolCodec.factory();
    }
    
    protected ParameterizedFactory<Pair<? extends Pair<Class<Message.ClientSession>, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>>, Connection<Message.ClientSession>>, ? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getDefaultConnectionFactory() {
        return PingingClient.factory(timeOut, runtime.getExecutors().get(ScheduledExecutorService.class));
    }
    
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getDefaultClientConnectionFactory() {
        ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnections = 
                clientModule.getClientConnectionFactory(
                            codecFactory, connectionFactory).get();
        return clientConnections;
    }
}
