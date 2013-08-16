package edu.uw.zookeeper.server;

import java.net.SocketAddress;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleServer extends ForwardingService {

    public static SimpleServer newInstance(
            IntraVmNetModule module) {
        return newInstance(module.factory().addresses().get(), module);
    }
    
    public static SimpleServer newInstance(
            SocketAddress address,
            NetServerModule module) {
        SimpleServerExecutor executor = SimpleServerExecutor.newInstance();
        ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory = 
                module.getServerConnectionFactory(
                        ServerApplicationModule.codecFactory(),
                        ServerApplicationModule.connectionFactory()).get(address);
        ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections = 
                ServerConnectionExecutorsService.newInstance(connectionFactory, executor.getTasks());
        return new SimpleServer(connections, executor);
    }
    
    protected final ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections;
    protected final SimpleServerExecutor executor;
    
    protected SimpleServer(
            ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections,
            SimpleServerExecutor executor) {
        this.connections = connections;
        this.executor = executor;
    }
    
    public ServerConnectionExecutorsService<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getConnections() {
        return connections;
    }
    
    public SimpleServerExecutor getExecutor() {
        return executor;
    }

    @Override
    protected Service delegate() {
        return connections;
    }
}
