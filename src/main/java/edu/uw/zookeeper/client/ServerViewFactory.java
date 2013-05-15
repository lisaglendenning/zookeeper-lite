package edu.uw.zookeeper.client;

import java.net.SocketAddress;

import edu.uw.zookeeper.ServerQuorumView;
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
import edu.uw.zookeeper.util.TimeValue;

public class ServerViewFactory implements DefaultsFactory<Session, ClientProtocolConnection> {

    public static ServerViewFactory newInstance(
            ClientConnectionFactory connections,
            ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ServerQuorumView view,
            TimeValue timeOut) {
        SocketAddress address = view.asAddress().get();
        Factory<Connection> connectionFactory = FixedClientConnectionFactory.newInstance(
                address, connections);
        ZxidTracker.Decorator zxids = 
                ZxidTracker.Decorator.newInstance(ClientCodecConnection.factory(connectionFactory, codecFactory));
        DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> delegate = 
                ClientProtocolConnection.factory(processor, zxids, zxids.asTracker(), timeOut);
        return new ServerViewFactory(zxids, delegate);
    }
    
    protected final ZxidTracker.Decorator zxids;
    protected final DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> delegate;

    protected ServerViewFactory(
            ZxidTracker.Decorator zxids,
            DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> delegate) {
        this.zxids = zxids;
        this.delegate = delegate;
    }

    @Override
    public ClientProtocolConnection get() {
        return delegate.get();
    }

    @Override
    public ClientProtocolConnection get(Session value) {
        return delegate.get(OpCreateSession.Request.RenewRequest.factory(zxids.asTracker(), value));
    }
}
