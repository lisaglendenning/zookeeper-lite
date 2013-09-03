package edu.uw.zookeeper.client;


import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ScheduledExecutorService;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;

public class ClientConnectionFactoryBuilder implements ZooKeeperApplication.RuntimeBuilder<ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>>> {

    public static ClientConnectionFactoryBuilder defaults() {
        return new ClientConnectionFactoryBuilder();
    }
    
    protected final RuntimeModule runtime;
    protected final NetClientModule clientModule;
    protected final TimeValue timeOut;
    protected final ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory;
    protected final ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> connectionFactory;
    
    public ClientConnectionFactoryBuilder() {
        this(null, null, null, null, null);
    }

    public ClientConnectionFactoryBuilder(
            RuntimeModule runtime,
            NetClientModule clientModule,
            TimeValue timeOut,
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> connectionFactory) {
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
            return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }
    
    public NetClientModule getClientModule() {
        return clientModule;
    }

    public ClientConnectionFactoryBuilder setClientModule(NetClientModule clientModule) {
        if (this.clientModule == clientModule) {
            return this;
        } else {
            return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public TimeValue getTimeOut() {
        return timeOut;
    }

    public ClientConnectionFactoryBuilder setTimeOut(TimeValue timeOut) {
        if (this.timeOut == timeOut) {
            return this;
        } else {
            return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return codecFactory;
    }

    public ClientConnectionFactoryBuilder setCodecFactory(
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory) {
        if (this.codecFactory == codecFactory) {
            return this;
        } else {
            return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    public ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getConnectionFactory() {
        return connectionFactory;
    }

    public ClientConnectionFactoryBuilder setConnectionFactory(
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> connectionFactory) {
        if (this.connectionFactory == connectionFactory) {
            return this;
        } else {
            return new ClientConnectionFactoryBuilder(runtime, clientModule, timeOut, codecFactory, connectionFactory);
        }
    }

    protected TimeValue getDefaultTimeOut() {
        return ZooKeeperApplication.ConfigurableTimeout.get(runtime.configuration());
    }
    
    protected NetClientModule getDefaultClientModule() {
        return NettyClientModule.newInstance(runtime);
    }
    
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getDefaultCodecFactory() {
        return AssignXidCodec.factory();
    }
    
    protected ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultConnectionFactory() {
        return PingingClient.factory(timeOut, runtime.executors().get(ScheduledExecutorService.class));
    }
    
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
        ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                clientModule.getClientConnectionFactory(
                            codecFactory, connectionFactory).get();
        return clientConnections;
    }

    @Override
    public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> build() {
        checkState(runtime != null);

        if (clientModule == null) {
            return setClientModule(getDefaultClientModule()).build();
        } else if (timeOut == null) {
            return setTimeOut(getDefaultTimeOut()).build();
        } else if (codecFactory == null) {
            return setCodecFactory(getDefaultCodecFactory()).build();
        } else if (connectionFactory == null) {
            return setConnectionFactory(getDefaultConnectionFactory()).build();
        } else {
            return getDefaultClientConnectionFactory();
        }
    }      
}
