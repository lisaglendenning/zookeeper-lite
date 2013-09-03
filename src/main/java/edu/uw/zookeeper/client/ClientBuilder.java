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

public class ClientBuilder implements ZooKeeperApplication.RuntimeBuilder<List<? extends Service>> {

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

    protected final ClientConnectionFactoryBuilder connectionBuilder;
    protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory;
    protected final ClientConnectionExecutorService clientExecutor;
    
    protected ClientBuilder() {
        this(ClientConnectionFactoryBuilder.defaults(), null, null);
    }

    protected ClientBuilder(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor) {
        this.connectionBuilder = connectionBuilder;
        this.clientConnectionFactory = clientConnectionFactory;
        this.clientExecutor = clientExecutor;
    }

    @Override
    public RuntimeModule getRuntimeModule() {
        return connectionBuilder.getRuntimeModule();
    }

    @Override
    public ClientBuilder setRuntimeModule(RuntimeModule runtime) {
        return new ClientBuilder(connectionBuilder.setRuntimeModule(runtime), clientConnectionFactory, clientExecutor);
    }
    
    public ClientConnectionFactoryBuilder getConnectionBuilder() {
        return connectionBuilder;
    }

    public ClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return new ClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutor);
    }
    
    public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClientConnectionFactory() {
        return clientConnectionFactory;
    }

    public ClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        return new ClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutor);
    }
    
    public ClientConnectionExecutorService getClientConnectionExecutor() {
        return clientExecutor;
    }

    public ClientBuilder setClientConnectionExecutor(
            ClientConnectionExecutorService clientExecutor) {
        return new ClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutor);
    }
    
    @Override
    public List<? extends Service> build() {
        return setDefaults().getServices();
    }
    
    public ClientBuilder setDefaults() {
        checkState(getRuntimeModule() != null);
    
        if (clientConnectionFactory == null) {
            return setClientConnectionFactory(getDefaultClientConnectionFactory());
        } else if (clientExecutor == null) {
            return setClientConnectionExecutor(getDefaultClientConnectionExecutorService());
        } else {
            return this;
        }
    }
    
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
        return connectionBuilder.build();
    }

    protected ClientConnectionExecutorService getDefaultClientConnectionExecutorService() {
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().configuration());
        final EnsembleViewFactory<? extends ServerViewFactory<Session, ?>> ensembleFactory = 
                EnsembleViewFactory.fromSession(
                    clientConnectionFactory,
                    ensemble, 
                    connectionBuilder.getTimeOut(),
                    getRuntimeModule().executors().get(ScheduledExecutorService.class));
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
