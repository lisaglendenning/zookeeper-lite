package edu.uw.zookeeper.client;

import java.net.SocketAddress;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.TimeValue;

public class ServerViewFactory implements DefaultsFactory<Session, ClientProtocolExecutor> {

    public static <C extends ClientCodecConnection> ServerViewFactory newInstance(
            ClientConnectionFactory<Message.ClientSessionMessage, C> connections,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ServerView.Address<? extends SocketAddress> server,
            TimeValue timeOut) {
        FixedClientConnectionFactory<Message.ClientSessionMessage, C> connectionFactory = 
                FixedClientConnectionFactory.newInstance(
                        server.get(), connections);
        ZxidTracker.Decorator<Message.ClientSessionMessage, C> zxids = 
                ZxidTracker.Decorator.newInstance(connectionFactory);
        DefaultsFactory<Factory<ConnectMessage.Request>, ClientProtocolExecutor> delegate = 
                ClientProtocolExecutor.factory(processor, zxids, zxids.first(), timeOut);
        return new ServerViewFactory(server, zxids, delegate);
    }
    
    public static class FixedClientConnectionFactory<I, C extends Connection<I>> extends AbstractPair<SocketAddress, ClientConnectionFactory<I,C>> implements Factory<C> {
        
        public static <I, C extends Connection<I>> FixedClientConnectionFactory<I,C> newInstance(SocketAddress address,
                ClientConnectionFactory<I,C> connectionFactory) {
            return new FixedClientConnectionFactory<I,C>(address, connectionFactory);
        }
        
        protected FixedClientConnectionFactory(SocketAddress address,
                ClientConnectionFactory<I,C> connectionFactory) {
            super(address, connectionFactory);
        }
        
        @Override
        public C get() {
            try {
                return second.connect(first).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    protected final ServerView.Address<? extends SocketAddress> server;
    protected final ZxidTracker.Decorator<Message.ClientSessionMessage, ? extends ClientCodecConnection> zxids;
    protected final DefaultsFactory<Factory<ConnectMessage.Request>, ClientProtocolExecutor> delegate;

    protected ServerViewFactory(
            ServerView.Address<? extends SocketAddress> server,
            ZxidTracker.Decorator<Message.ClientSessionMessage, ? extends ClientCodecConnection> zxids,
            DefaultsFactory<Factory<ConnectMessage.Request>, ClientProtocolExecutor> delegate) {
        this.zxids = zxids;
        this.delegate = delegate;
        this.server = server;
    }
    
    public ServerView.Address<? extends SocketAddress> server() {
        return server;
    }
    
    public ZxidTracker zxids() {
        return zxids.first();
    }

    @Override
    public ClientProtocolExecutor get() {
        return delegate.get();
    }

    @Override
    public ClientProtocolExecutor get(Session session) {
        return delegate.get(ConnectMessage.Request.RenewRequest.factory(zxids(), session));
    }
}
