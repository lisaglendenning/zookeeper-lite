package edu.uw.zookeeper.client;

import java.net.SocketAddress;

import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.FixedClientConnectionFactory;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.TimeValue;

public class ServerViewFactory implements DefaultsFactory<Session, ClientProtocolConnection> {

    public static ServerViewFactory newInstance(
            ClientConnectionFactory connections,
            Factory<Publisher> publishers,
            ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ServerView.Address<? extends SocketAddress> server,
            TimeValue timeOut) {
        Factory<Connection> connectionFactory = FixedClientConnectionFactory.newInstance(
                server.get(), connections);
        ZxidTracker.Decorator zxids = 
                ZxidTracker.Decorator.newInstance(ClientCodecConnection.factory(connectionFactory, codecFactory));
        DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> delegate = 
                ClientProtocolConnection.factory(processor, publishers, zxids, zxids.asTracker(), timeOut);
        return new ServerViewFactory(server, zxids, delegate);
    }
    
    protected final ServerView.Address<? extends SocketAddress> server;
    protected final ZxidTracker.Decorator zxids;
    protected final DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> delegate;

    protected ServerViewFactory(
            ServerView.Address<? extends SocketAddress> server,
            ZxidTracker.Decorator zxids,
            DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> delegate) {
        this.zxids = zxids;
        this.delegate = delegate;
        this.server = server;
    }
    
    public ServerView.Address<? extends SocketAddress> server() {
        return server;
    }
    
    public ZxidTracker zxids() {
        return zxids.asTracker();
    }

    @Override
    public ClientProtocolConnection get() {
        return delegate.get();
    }

    @Override
    public ClientProtocolConnection get(Session session) {
        return delegate.get(OpCreateSession.Request.RenewRequest.factory(zxids(), session));
    }
}
