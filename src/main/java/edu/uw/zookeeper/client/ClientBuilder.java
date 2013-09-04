package edu.uw.zookeeper.client;


import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

public class ClientBuilder implements ZooKeeperApplication.RuntimeBuilder<List<Service>, ClientBuilder> {

    public static ClientBuilder defaults() {
        return new ClientBuilder();
    }
    
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

    protected final RuntimeModule runtime;
    protected final ClientConnectionFactoryBuilder connectionBuilder;
    protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory;
    protected final ClientConnectionExecutorService clientExecutor;
    
    protected ClientBuilder() {
        this(null, null, null, null);
    }

    protected ClientBuilder(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        this.runtime = runtime;
        this.connectionBuilder = connectionBuilder;
        this.clientConnectionFactory = clientConnectionFactory;
        this.clientExecutor = clientExecutor;
    }

    @Override
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Override
    public ClientBuilder setRuntimeModule(RuntimeModule runtime) {
        if (this.runtime == runtime) {
            return this;
        } else {
            return newInstance(
                    (connectionBuilder == null) ? connectionBuilder : connectionBuilder.setRuntimeModule(runtime), 
                    clientConnectionFactory, 
                    clientExecutor,
                    runtime);
        }
    }
    
    public ClientConnectionFactoryBuilder getConnectionBuilder() {
        return connectionBuilder;
    }

    public ClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        if (this.connectionBuilder == connectionBuilder) {
            return this;
        } else {
            return newInstance(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        }
    }
    
    public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClientConnectionFactory() {
        return clientConnectionFactory;
    }

    public ClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        if (this.clientConnectionFactory == clientConnectionFactory) {
            return this;
        } else {
            return newInstance(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        }
    }
    
    public ClientConnectionExecutorService getClientConnectionExecutor() {
        return clientExecutor;
    }

    public ClientBuilder setClientConnectionExecutor(
            ClientConnectionExecutorService clientExecutor) {
        if (this.clientExecutor == clientExecutor) {
            return this;
        } else {
            return newInstance(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        }
    }

    @Override
    public ClientBuilder setDefaults() {
        checkState(getRuntimeModule() != null);
    
        if (this.connectionBuilder == null) {
            return setConnectionBuilder(getDefaultClientConnectionFactoryBuilder()).setDefaults();
        }
        ClientConnectionFactoryBuilder connectionBuilder = this.connectionBuilder.setDefaults();
        if (this.connectionBuilder != connectionBuilder) {
            return setConnectionBuilder(connectionBuilder).setDefaults();
        }
        if (clientConnectionFactory == null) {
            return setClientConnectionFactory(getDefaultClientConnectionFactory()).setDefaults();
        }
        if (clientExecutor == null) {
            return setClientConnectionExecutor(getDefaultClientConnectionExecutorService()).setDefaults();
        }
        return this;
    }

    @Override
    public List<Service> build() {
        return setDefaults().getServices();
    }
    
    protected ClientBuilder newInstance(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        return new ClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }
    
    protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
        return ClientConnectionFactoryBuilder.defaults().setRuntimeModule(runtime).setDefaults();
    }
    
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
        return connectionBuilder.build();
    }

    protected ClientConnectionExecutorService getDefaultClientConnectionExecutorService() {
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
        final EnsembleViewFactory<? extends ServerViewFactory<Session, ?>> ensembleFactory = 
                EnsembleViewFactory.fromSession(
                    clientConnectionFactory,
                    ensemble, 
                    connectionBuilder.getTimeOut(),
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
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

    protected List<Service> getServices() {
        return Lists.<Service>newArrayList(
                clientConnectionFactory, 
                clientExecutor);
    }   
}
