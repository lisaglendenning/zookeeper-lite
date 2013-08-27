package edu.uw.zookeeper.client;


import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.PingingClient;

public class ClientApplicationModule implements Callable<Application> {

    public static ParameterizedFactory<RuntimeModule, Application> factory() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                try {
                    return new ClientApplicationModule(runtime).call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }  
        };
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
    
    public ClientApplicationModule(RuntimeModule runtime) {
        this.runtime = runtime;
    }
    
    public RuntimeModule getRuntime() {
        return runtime;
    }

    @Override
    public Application call() throws Exception {
        return ServiceApplication.newInstance(createServices());
    }
    
    protected NetClientModule getNetClientModule() {
        return NettyClientModule.newInstance(runtime);
    }
    
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return AssignXidCodec.factory();
    }
    
    protected TimeValue getTimeOut() {
        TimeValue value = DefaultMain.ConfigurableTimeout.get(runtime.configuration());
        return value;
    }
    
    protected ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(TimeValue timeOut) {
        NetClientModule clientModule = getNetClientModule();
        ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = getCodecFactory();
        ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                PingingClient.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                clientModule.getClientConnectionFactory(
                            codecFactory, pingingFactory).get();
        runtime.serviceMonitor().add(clientConnections);
        return clientConnections;
    }
    
    protected ClientConnectionExecutorService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionExecutorService(TimeValue timeOut) {
        ClientConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = getClientConnectionFactory(timeOut);
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(runtime.configuration());
        final EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<Session, ServerInetAddressView, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>> ensembleFactory = 
                EnsembleViewFactory.newInstance(
                    clientConnections, 
                    ServerInetAddressView.class, 
                    ensemble, 
                    timeOut,
                    runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionExecutorService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> executor =
                ClientConnectionExecutorService.newInstance(
                        new Factory<ListenableFuture<ClientConnectionExecutor<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>>() {
                            @Override
                            public ListenableFuture<ClientConnectionExecutor<PingingClient<Request, AssignXidCodec, Connection<Request>>>> get() {
                                return ensembleFactory.get().get();
                            }
                        });
        runtime.serviceMonitor().add(executor);
        return executor;
    }
    
    protected ServiceMonitor createServices() {
        getClientConnectionExecutorService(getTimeOut());
        return runtime.serviceMonitor();
    }
}
