package edu.uw.zookeeper.server;

import java.net.InetSocketAddress;

import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.intravm.IntraVmClientConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmTest;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;

public class SimpleServerConnections extends ServerConnectionExecutorsService<IntraVmConnection<InetSocketAddress>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>> {

    public static <C extends Connection<? super Message.Server>> ParameterizedFactory<C, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> codecFactory() {
        return new ParameterizedFactory<C, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>>() {
            @Override
            public ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C> get(C value) {
                return ProtocolCodecConnection.newInstance(ServerProtocolCodec.newInstance(value), value);
            }
        };
    }
    
    public static IntraVmServerConnectionFactory<InetSocketAddress, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>> newServerConnectionFactory(int port) {
        ParameterizedFactory<IntraVmConnection<InetSocketAddress>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>> codecFactory = codecFactory();
        return IntraVmTest.newServerFactory(port, codecFactory);
    }
    
    public static SimpleServerConnections newInstance(
            ServerTaskExecutor executor) {
        ParameterizedFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>, ServerConnectionExecutor<IntraVmConnection<InetSocketAddress>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>>> factory = 
                ServerConnectionExecutor.factory(
                        executor.getAnonymousExecutor(), 
                        executor.getConnectExecutor(), 
                        executor.getSessionExecutor());
        return new SimpleServerConnections(
                newServerConnectionFactory(0), factory);
    }
    
    public SimpleServerConnections(
            IntraVmServerConnectionFactory<InetSocketAddress, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>> connections,
            ParameterizedFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>, ServerConnectionExecutor<IntraVmConnection<InetSocketAddress>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>>> factory) {
        super(connections, factory);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IntraVmServerConnectionFactory<InetSocketAddress, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>> connections() {
        return (IntraVmServerConnectionFactory<InetSocketAddress, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, IntraVmConnection<InetSocketAddress>>>) connections;
    }
    
    public <I, C extends Connection<I>> IntraVmClientConnectionFactory<InetSocketAddress, C> clients(
            ParameterizedFactory<IntraVmConnection<InetSocketAddress>, C> connectionFactory) {
        return IntraVmTest.newClientFactory(connections(), connectionFactory);
    }
}
