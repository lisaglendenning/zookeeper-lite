package edu.uw.zookeeper;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.EnsembleViewConfiguration;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.server.ClientAddressConfiguration;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class SimpleServerAndClient implements ZooKeeperApplication.RuntimeBuilder<List<Service>, SimpleServerAndClient> {

    public static SimpleServerAndClient defaults() {
        return new SimpleServerAndClient();
    }
    
    protected final RuntimeModule runtime;
    protected final IntraVmNetModule netModule;
    protected final SimpleServerBuilder<?> serverBuilder;
    protected final ConnectionClientExecutorService.Builder clientBuilder;

    protected SimpleServerAndClient() {
        this(null, null, null, null);
    }
    
    protected SimpleServerAndClient(
            IntraVmNetModule netModule,
            SimpleServerBuilder<?> server,
            ConnectionClientExecutorService.Builder client,
            RuntimeModule runtime) {
        this.netModule = netModule;
        this.serverBuilder = server;
        this.clientBuilder = client;
        this.runtime = runtime;
    }
    
    @Override
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Override
    public SimpleServerAndClient setRuntimeModule(
            RuntimeModule runtime) {
        if (this.runtime == runtime) {
            return this;
        } else {
            return newInstance(
                    netModule, 
                    (serverBuilder == null) ? serverBuilder : serverBuilder.setRuntimeModule(runtime), 
                    (clientBuilder == null) ? clientBuilder : clientBuilder.setRuntimeModule(runtime), 
                    runtime);
        }
    }

    public IntraVmNetModule getNetModule() {
        return netModule;
    }

    public SimpleServerAndClient setNetModule(IntraVmNetModule netModule) {
        if (this.netModule == netModule) {
            return this;
        } else {
            return newInstance(netModule, serverBuilder, clientBuilder, runtime);
        }
    }

    public SimpleServerBuilder<?> getServerBuilder() {
        return serverBuilder;
    }
    
    public SimpleServerAndClient setServerBuilder(SimpleServerBuilder<?> serverBuilder) {
        if (this.serverBuilder == serverBuilder) {
            return this;
        } else {
            return newInstance(netModule, serverBuilder, clientBuilder, runtime);
        }
    }

    public ConnectionClientExecutorService.Builder getClientBuilder() {
        return clientBuilder;
    }
    
    public SimpleServerAndClient setClientBuilder(ConnectionClientExecutorService.Builder clientBuilder) {
        if (this.clientBuilder == clientBuilder) {
            return this;
        } else {
            return newInstance(netModule, serverBuilder, clientBuilder, runtime);
        }
    }
    
    @Override
    public SimpleServerAndClient setDefaults() {
        if (runtime == null) {
            return setRuntimeModule(getDefaultRuntimeModule()).setDefaults();
        }
        if (netModule == null) {
            return setNetModule(getDefaultNetModule()).setDefaults();
        }
        if (serverBuilder == null) {
            return setServerBuilder(getDefaultServerBuilder().setDefaults()).setDefaults();
        }
        if (clientBuilder == null) {
            return setClientBuilder(getDefaultClientBuilder().setDefaults()).setDefaults();
        }
        return this;
    }

    @Override
    public List<Service> build() {
        return setDefaults().getServices();
    }
    
    protected SimpleServerAndClient newInstance(
            IntraVmNetModule netModule,
            SimpleServerBuilder<?> serverBuilder,
            ConnectionClientExecutorService.Builder clientBuilder,
            RuntimeModule runtime) {
        return new SimpleServerAndClient(netModule, serverBuilder, clientBuilder, runtime);
    }

    protected RuntimeModule getDefaultRuntimeModule() {
        return DefaultRuntimeModule.defaults();
    }
    
    protected IntraVmNetModule getDefaultNetModule() {
        return IntraVmNetModule.defaults();
    }
    
    protected SimpleServerBuilder<?> getDefaultServerBuilder() {
        ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) getNetModule().factory().addresses().get());
        ClientAddressConfiguration.set(getRuntimeModule().getConfiguration(), address);
        ServerConnectionFactoryBuilder connections = ServerConnectionFactoryBuilder.defaults().setServerModule(getNetModule());
        return SimpleServerBuilder.fromConnections(ServerConnectionsHandler.Builder.withConnectionBuilder(connections)).setRuntimeModule(getRuntimeModule());
    }
    
    protected ConnectionClientExecutorService.Builder getDefaultClientBuilder() {
        EnsembleViewConfiguration.set(getRuntimeModule().getConfiguration(), EnsembleView.copyOf(getServerBuilder().getConnectionsBuilder().getConnectionBuilder().getAddress()));
        return ConnectionClientExecutorService.builder().setConnectionBuilder(ClientConnectionFactoryBuilder.defaults().setClientModule(getNetModule())).setRuntimeModule(getRuntimeModule());
    }

    protected List<Service> getServices() {
        List<Service> services = Lists.newLinkedList();
        services.addAll(serverBuilder.build());
        services.addAll(clientBuilder.build());
        return services;
    }
}