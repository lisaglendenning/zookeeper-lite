package edu.uw.zookeeper.client;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

public abstract class SimpleClient<T extends Application> extends ClientApplicationBuilder<T> {
    
    protected final ServerInetAddressView serverAddress;
    
    protected SimpleClient(
            ServerInetAddressView serverAddress,
            NetClientModule clientModule,
            IntraVmNetModule net) {
        this.serverAddress = serverAddress;
        this.clientModule = clientModule;
    }

    @Override
    protected TimeValue getDefaultTimeOut() {
        return TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT);
    }
    
    @Override
    protected ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultConnectionFactory() {
        return ProtocolCodecConnection.<Operation.Request,AssignXidCodec,Connection<Operation.Request>>factory();
    }
    
    @Override    
    protected ClientConnectionExecutorService getDefaultClientConnectionExecutorService() {
        Factory<? extends ListenableFuture<? extends ClientConnectionExecutor<?>>> factory = 
                ServerViewFactory.newInstance(
                        clientConnectionFactory, 
                        serverAddress, 
                        timeOut, 
                        runtime.executors().get(ScheduledExecutorService.class));
        ClientConnectionExecutorService service =
                ClientConnectionExecutorService.newInstance(
                        factory);
        return service;
    }
}
