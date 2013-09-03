package edu.uw.zookeeper;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.SimpleClientBuilder;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class SimpleClient implements ZooKeeperApplication.RuntimeBuilder<List<Service>> {

    public static SimpleClient defaults() {
        return new SimpleClient();
    }
    
    protected final RuntimeModule runtime;
    protected final IntraVmNetModule netModule;
    protected final SimpleServerBuilder serverBuilder;
    protected final SimpleClientBuilder clientBuilder;

    protected SimpleClient() {
        this(null, null, null, null);
    }
    
    protected SimpleClient(
            IntraVmNetModule netModule,
            SimpleServerBuilder server,
            SimpleClientBuilder client,
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
    public SimpleClient setRuntimeModule(
            RuntimeModule runtime) {
        if (this.runtime == runtime) {
            return this;
        } else {
            return new SimpleClient(netModule, serverBuilder.setRuntimeModule(runtime), clientBuilder.setRuntimeModule(runtime), runtime);
        }
    }

    public IntraVmNetModule getNetModule() {
        return netModule;
    }

    public SimpleClient setNetModule(IntraVmNetModule netModule) {
        if (this.netModule == netModule) {
            return this;
        } else {
            return new SimpleClient(netModule, serverBuilder, clientBuilder, runtime);
        }
    }

    public SimpleServerBuilder getServerBuilder() {
        return serverBuilder;
    }
    
    public SimpleClient setServerBuilder(SimpleServerBuilder serverBuilder) {
        if (this.serverBuilder == serverBuilder) {
            return this;
        } else {
            return new SimpleClient(netModule, serverBuilder, clientBuilder, runtime);
        }
    }

    public SimpleClientBuilder getClientBuilder() {
        return clientBuilder;
    }
    
    public SimpleClient setClientBuilder(SimpleClientBuilder clientBuilder) {
        if (this.clientBuilder == clientBuilder) {
            return this;
        } else {
            return new SimpleClient(netModule, serverBuilder, clientBuilder, runtime);
        }
    }
    
    @Override
    public List<Service> build() {
        return setDefaults().getServices();
    }

    public SimpleClient setDefaults() {
        if (runtime == null) {
            return setRuntimeModule(getDefaultRuntimeModule());
        } else if (netModule == null) {
            return setNetModule(getDefaultNetModule());
        } else if (serverBuilder == null) {
            return setServerBuilder(getDefaultServerBuilder().setDefaults());
        } else if (clientBuilder == null) {
            return setClientBuilder(getDefaultClientBuilder().setDefaults());
        } else {
            return this;
        }
    }
    
    protected RuntimeModule getDefaultRuntimeModule() {
        return DefaultRuntimeModule.defaults();
    }
    
    protected IntraVmNetModule getDefaultNetModule() {
        return IntraVmNetModule.defaults();
    }
    
    protected SimpleServerBuilder getDefaultServerBuilder() {
        ServerInetAddressView address = ServerInetAddressView.of((InetSocketAddress) netModule.factory().addresses().get());
        return SimpleServerBuilder.defaults(address, netModule).setRuntimeModule(runtime);
    }
    
    protected SimpleClientBuilder getDefaultClientBuilder() {
        return SimpleClientBuilder.defaults(serverBuilder.getConnectionBuilder().getAddress(), netModule).setRuntimeModule(runtime);
    }

    protected List<Service> getServices() {
        List<Service> services = Lists.newLinkedList();
        services.addAll(serverBuilder.build());
        services.addAll(clientBuilder.build());
        return services;
    }
}