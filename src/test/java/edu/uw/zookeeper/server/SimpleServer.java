package edu.uw.zookeeper.server;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public abstract class SimpleServer<T extends Application> extends ServerApplicationBuilder<T> {

    protected final ServerInetAddressView address;
    protected ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors;
    
    protected SimpleServer(
            ServerInetAddressView address,
            NetServerModule serverModule) {
        this.address = address;
        this.serverModule = serverModule;
    }
    
    public ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getConnectionExecutors() {
        return connectionExecutors;
    }
    
    public SimpleServerExecutor getServerTaskExecutor() {
        return (SimpleServerExecutor) serverTaskExecutor;
    }
    
    @Override
    protected void getDefaults() {
        super.getDefaults();
        
        if (connectionExecutors == null) {
            connectionExecutors = getDefaultServerConnectionExecutorsService();
        }
    }
    
    @Override
    protected TimeValue getDefaultTimeOut() {
        return TimeValue.create(Session.Parameters.NEVER_TIMEOUT, Session.Parameters.TIMEOUT_UNIT);
    }
    
    @Override
    protected ServerInetAddressView getDefaultAddress() {
        return address;
    }
    
    @Override
    protected SimpleServerExecutor getDefaultServerTaskExecutor() {
        return SimpleServerExecutor.newInstance();
    }
}
