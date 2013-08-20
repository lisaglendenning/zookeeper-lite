package edu.uw.zookeeper.client;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.server.SimpleServer;

public class SimpleClient extends ForwardingService {
    
    public static SimpleClient newInstance() {
        RuntimeModule runtime = DefaultRuntimeModule.newInstance();
        IntraVmNetModule net = IntraVmNetModule.defaults();
        return new SimpleClient(runtime, net);
    }

    protected final RuntimeModule runtime;
    protected final SimpleServer server;
    protected final ClientConnectionExecutorService<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> client;
    
    public SimpleClient(
            RuntimeModule runtime,
            IntraVmNetModule net) {
        this.runtime = runtime;
        this.server = SimpleServer.newInstance(net, runtime.executors().asScheduledExecutorServiceFactory().get());
        runtime.serviceMonitor().add(server);
        ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = net.getClientConnectionFactory(
                ClientApplicationModule.codecFactory(), 
                ProtocolCodecConnection.<Operation.Request,AssignXidCodec,Connection<Operation.Request>>factory()).get();
        runtime.serviceMonitor().add(clientConnections);
        ServerViewFactory<Session, ServerInetAddressView, ? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientFactory = ServerViewFactory.newInstance(
                clientConnections, 
                ServerInetAddressView.of((InetSocketAddress) server.getConnections().connections().listenAddress()), 
                TimeValue.create(0L, TimeUnit.MILLISECONDS), 
                runtime.executors().asScheduledExecutorServiceFactory().get());
        this.client = ClientConnectionExecutorService.newInstance(clientFactory);
        runtime.serviceMonitor().add(client);
    }
    
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }
    
    public SimpleServer getServer() {
        return server;
    }
    
    public ClientConnectionExecutorService<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClient() {
        return client;
    }
    
    @Override
    protected Service delegate() {
        return runtime.serviceMonitor();
    }

}
