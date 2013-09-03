package edu.uw.zookeeper.client;


import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.PingingClient;

public abstract class ClientApplicationBuilder<T extends Application> extends ZooKeeperApplication.ApplicationBuilder<T> {

    @Configurable(arg="ensemble", key="Ensemble", value="localhost:2181", help="Address:Port,...")
    public static class ConfigurableEnsembleView implements Function<Configuration, EnsembleView<ServerInetAddressView>> {

        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
        
        @Override
        public EnsembleView<ServerInetAddressView> apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return EnsembleView.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }
    }

    protected NetClientModule clientModule;
    protected TimeValue timeOut;
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory;
    protected ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> connectionFactory;
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory;
    
    protected ClientApplicationBuilder() {
        super();
    }
    
    public NetClientModule getClientModule() {
        return clientModule;
    }

    public ClientApplicationBuilder<T> setClientModule(NetClientModule clientModule) {
        this.clientModule = clientModule;
        return this;
    }

    public TimeValue getTimeOut() {
        return timeOut;
    }

    public ClientApplicationBuilder<T> setTimeOut(TimeValue timeOut) {
        this.timeOut = timeOut;
        return this;
    }

    public ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return codecFactory;
    }

    public ClientApplicationBuilder<T> setCodecFactory(
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory) {
        this.codecFactory = codecFactory;
        return this;
    }

    public ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getConnectionFactory() {
        return connectionFactory;
    }

    public ClientApplicationBuilder<T> setConnectionFactory(
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClientConnectionFactory() {
        return clientConnectionFactory;
    }

    public ClientApplicationBuilder<T> setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        this.clientConnectionFactory = clientConnectionFactory;
        return this;
    }

    protected TimeValue getDefaultTimeOut() {
        return ZooKeeperApplication.ConfigurableTimeout.get(runtime.configuration());
    }
    
    protected NetClientModule getDefaultNetClientModule() {
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

    protected ClientConnectionExecutorService getDefaultClientConnectionExecutorService() {
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(runtime.configuration());
        final EnsembleViewFactory<? extends ServerViewFactory<Session, ?>> ensembleFactory = 
                EnsembleViewFactory.newInstance(
                    clientConnectionFactory,
                    ensemble, 
                    timeOut,
                    runtime.executors().get(ScheduledExecutorService.class));
        ClientConnectionExecutorService service =
                ClientConnectionExecutorService.newInstance(
                        new Factory<ListenableFuture<? extends ClientConnectionExecutor<?>>>() {
                            @Override
                            public ListenableFuture<? extends ClientConnectionExecutor<?>> get() {
                                return ensembleFactory.get().get();
                            }
                        });
        return service;
    }

    @Override
    public T build() {
        getDefaults();
        return getApplication();
    }
    
    protected void getDefaults() {
        checkState(runtime != null);

        if (clientModule == null) {
            clientModule = getDefaultNetClientModule();
        }
        
        if (timeOut == null) {
            timeOut = getDefaultTimeOut();
        }
        
        if (codecFactory == null) {
            codecFactory = getDefaultCodecFactory();
        }
        
        if (connectionFactory == null) {
            connectionFactory = getDefaultConnectionFactory();
        }
        
        if (clientConnectionFactory == null) {
            clientConnectionFactory = getDefaultClientConnectionFactory();
        }
    }
    
    protected abstract T getApplication();
}
