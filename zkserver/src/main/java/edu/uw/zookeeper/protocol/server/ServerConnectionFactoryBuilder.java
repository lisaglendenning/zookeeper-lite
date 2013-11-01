package edu.uw.zookeeper.protocol.server;

import static com.google.common.base.Preconditions.checkState;

import java.net.SocketAddress;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.*;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.server.ConfigurableServerAddressView;

public class ServerConnectionFactoryBuilder implements ZooKeeperApplication.RuntimeBuilder<ServerConnectionFactory<? extends ServerProtocolConnection<?,?>>, ServerConnectionFactoryBuilder> {

    public static ServerConnectionFactoryBuilder defaults() {
        return new ServerConnectionFactoryBuilder(null, null, null, null, null);
    }
    
    protected final RuntimeModule runtime;
    protected final NetServerModule serverModule;
    protected final Factory<? extends ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>> codecFactory;
    protected final ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ? extends ServerProtocolConnection<?,?>> connectionFactory;
    protected final ServerInetAddressView address;
    
    public ServerConnectionFactoryBuilder(
            RuntimeModule runtime,
            NetServerModule serverModule,
            Factory<? extends ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>> codecFactory,
            ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ? extends ServerProtocolConnection<?,?>> connectionFactory,
            ServerInetAddressView address) {
        this.runtime = runtime;
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
            return newInstance(runtime, serverModule, codecFactory, connectionFactory, address);
        }
    }
    
    public NetServerModule getServerModule() {
        return serverModule;
    }

    public ServerConnectionFactoryBuilder setServerModule(NetServerModule serverModule) {
        if (this.serverModule == serverModule) {
            return this;
        } else {
        return newInstance(runtime, serverModule, codecFactory, connectionFactory, address);
        }
    }

    public Factory<? extends ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>> getCodecFactory() {
        return codecFactory;
    }

    public ServerConnectionFactoryBuilder setCodecFactory(
            Factory<? extends ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>> codecFactory) {
        if (this.codecFactory == codecFactory) {
            return this;
        } else {
        return newInstance(runtime, serverModule, codecFactory, connectionFactory, address);
        }
    }

    public ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ? extends ServerProtocolConnection<?,?>> getConnectionFactory() {
        return connectionFactory;
    }

    public ServerConnectionFactoryBuilder setConnectionFactory(
            ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ? extends ServerProtocolConnection<?,?>> connectionFactory) {
        return newInstance(runtime, serverModule, codecFactory, connectionFactory, address);
    }
    
    public ServerInetAddressView getAddress() {
        return address;
    }
    
    public ServerConnectionFactoryBuilder setAddress(ServerInetAddressView address) {
        if (this.address == address) {
            return this;
        } else {
            return newInstance(runtime, serverModule, codecFactory, connectionFactory, address);
        }
    }

    @Override
    public ServerConnectionFactoryBuilder setDefaults() {
        checkState(runtime != null);
        
        if (serverModule == null) {
            return setServerModule(getDefaultServerModule()).setDefaults();
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
    public ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> build() {
        return setDefaults().getDefaultServerConnectionFactory();
    }
    
    protected ServerConnectionFactoryBuilder newInstance(
            RuntimeModule runtime,
            NetServerModule serverModule,
            Factory<? extends ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>> codecFactory,
            ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ? extends ServerProtocolConnection<?,?>> connectionFactory,
            ServerInetAddressView address) {
        return new ServerConnectionFactoryBuilder(runtime, serverModule, codecFactory, connectionFactory, address);
    }

    protected NetServerModule getDefaultServerModule() {
        return NettyServerModule.newInstance(runtime);
    }

    protected Factory<? extends ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>> getDefaultCodecFactory() {
        return new Factory<ServerProtocolCodec>() {
            @Override
            public ServerProtocolCodec get() {
                return ServerProtocolCodec.defaults();
            }
        };
    }
    
    protected ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ? extends ServerProtocolConnection<?,?>> getDefaultConnectionFactory() {
        return new ParameterizedFactory<CodecConnection<Message.Server,Message.Client,ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>,?>, ServerProtocolConnection<?,?>>() {
            @Override
            public ServerProtocolConnection<?,?> get(
                    CodecConnection<Message.Server, Message.Client, ProtocolCodec<Message.Server, Message.Client, Message.Server, Message.Client>, ?> value) {
                return ServerProtocolConnection.newInstance(value);
            }
        };
    }
    
    protected ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> getDefaultServerConnectionFactory() {
        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<? extends ServerProtocolConnection<?,?>>> serverConnectionFactory = 
                serverModule.getServerConnectionFactory(
                        codecFactory,
                        connectionFactory);
        ServerConnectionFactory<? extends ServerProtocolConnection<?, ?>> serverConnections = 
                serverConnectionFactory.get(address.get());
        return serverConnections;
    }
    
    protected ServerInetAddressView getDefaultAddress() {
        return ConfigurableServerAddressView.get(runtime.getConfiguration());
    }
}
