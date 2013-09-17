package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkState;

import java.net.SocketAddress;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.*;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;

public class ServerConnectionFactoryBuilder implements ZooKeeperApplication.RuntimeBuilder<ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>, ServerConnectionFactoryBuilder> {

    public static ServerConnectionFactoryBuilder defaults() {
        return new ServerConnectionFactoryBuilder();
    }
    
    protected final RuntimeModule runtime;
    protected final TimeValue timeOut;
    protected final NetServerModule serverModule;
    protected final ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory;
    protected final ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory;
    protected final ServerInetAddressView address;
    
    public ServerConnectionFactoryBuilder() {
        this(null, null, null, null, null, null);
    }

    public ServerConnectionFactoryBuilder(
            RuntimeModule runtime,
            TimeValue timeOut,
            NetServerModule serverModule,
            ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory,
            ServerInetAddressView address) {
        this.runtime = runtime;
        this.timeOut = timeOut;
        this.serverModule = serverModule;
        this.codecFactory = codecFactory;
        this.connectionFactory = connectionFactory;
        this.address = address;
    }
    
    @Override
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Override
    public ServerConnectionFactoryBuilder setRuntimeModule(RuntimeModule runtime) {
        if (this.runtime == runtime) {
            return this;
        } else {
            return newInstance(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
        }
    }
    
    public TimeValue getTimeOut() {
        return timeOut;
    }

    public ServerConnectionFactoryBuilder setTimeOut(TimeValue timeOut) {
        if (this.timeOut == timeOut) {
            return this;
        } else {
            return newInstance(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
        }
    }

    public NetServerModule getServerModule() {
        return serverModule;
    }

    public ServerConnectionFactoryBuilder setServerModule(NetServerModule serverModule) {
        if (this.serverModule == serverModule) {
            return this;
        } else {
        return newInstance(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
        }
    }

    public ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> getCodecFactory() {
        return codecFactory;
    }

    public ServerConnectionFactoryBuilder setCodecFactory(
            ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory) {
        if (this.codecFactory == codecFactory) {
            return this;
        } else {
        return newInstance(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
        }
    }

    public ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getConnectionFactory() {
        return connectionFactory;
    }

    public ServerConnectionFactoryBuilder setConnectionFactory(
            ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory) {
        if (this.connectionFactory == connectionFactory) {
            return this;
        } else {
        return newInstance(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
        }
    }
    
    public ServerInetAddressView getAddress() {
        return address;
    }
    
    public ServerConnectionFactoryBuilder setAddress(ServerInetAddressView address) {
        if (this.address == address) {
            return this;
        } else {
            return newInstance(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
        }
    }

    @Override
    public ServerConnectionFactoryBuilder setDefaults() {
        checkState(runtime != null);
        
        if (serverModule == null) {
            return setServerModule(getDefaultServerModule()).setDefaults();
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
        if (address == null) {
            return setAddress(getDefaultAddress()).setDefaults();
        }
        return this;
    }

    @Override
    public ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> build() {
        return setDefaults().getDefaultServerConnectionFactory();
    }
    
    protected ServerConnectionFactoryBuilder newInstance(
            RuntimeModule runtime,
            TimeValue timeOut,
            NetServerModule serverModule,
            ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory,
            ServerInetAddressView address) {
        return new ServerConnectionFactoryBuilder(runtime, timeOut, serverModule, codecFactory, connectionFactory, address);
    }

    protected TimeValue getDefaultTimeOut() {
        return ZooKeeperApplication.ConfigurableTimeout.get(runtime.getConfiguration());
    }
    
    protected NetServerModule getDefaultServerModule() {
        return NettyServerModule.newInstance(runtime);
    }

    protected ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> getDefaultCodecFactory() {
        return ServerProtocolCodec.factory();
    }
    
    protected ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultConnectionFactory() {
        return ProtocolCodecConnection.factory();
    }
    
    protected ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultServerConnectionFactory() {
        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>> serverConnectionFactory = 
                serverModule.getServerConnectionFactory(
                        codecFactory,
                        connectionFactory);
        ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                serverConnectionFactory.get(address.get());
        return serverConnections;
    }
    
    protected ServerInetAddressView getDefaultAddress() {
        return ConfigurableServerAddressView.get(runtime.getConfiguration());
    }
}
