package edu.uw.zookeeper.client.console;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

public class ConsoleClientBuilder extends ClientBuilder {

    public static ConsoleClientBuilder defaults() {
        return new ConsoleClientBuilder();
    }
    
    public ConsoleClientBuilder() {
        this(null, null, null, null);
    }

    protected ConsoleClientBuilder(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Request, AssignXidCodec, Connection<Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        super(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }

    @Override
    public ConsoleClientBuilder setRuntimeModule(RuntimeModule runtime) {
        return (ConsoleClientBuilder) super.setRuntimeModule(runtime);
    }

    @Override
    public ConsoleClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return (ConsoleClientBuilder) super.setConnectionBuilder(connectionBuilder);
    }

    @Override
    public ConsoleClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        return (ConsoleClientBuilder) super.setClientConnectionFactory(clientConnectionFactory);
    }

    @Override
    public ConsoleClientBuilder setClientConnectionExecutor(
            ClientConnectionExecutorService clientExecutor) {
        return (ConsoleClientBuilder) super.setClientConnectionExecutor(clientExecutor);
    }

    @Override
    public ConsoleClientBuilder setDefaults() {
        ConsoleClientBuilder builder = (ConsoleClientBuilder) super.setDefaults();

        return builder;
    }

    @Override
    protected ConsoleClientBuilder newInstance(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor,
            RuntimeModule runtime) {
        return new ConsoleClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
    }
    
    @Override
    protected List<Service> getServices() {
        List<Service> services = super.getServices();
        try {
            services.add(ConsoleReaderService.newInstance(clientExecutor));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return services;
    }   
}
