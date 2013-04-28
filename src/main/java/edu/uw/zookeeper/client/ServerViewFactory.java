package edu.uw.zookeeper.client;

import java.net.SocketAddress;

import edu.uw.zookeeper.ServerQuorumView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.FixedClientConnectionFactory;
import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TimeValue;

public class ServerViewFactory extends NewClientProtocolExecutorFactory {

    public static class ZxidTrackingDecorator implements Factory<ClientCodecConnection> {
        public static ZxidTrackingDecorator newInstance(Factory<? extends ClientCodecConnection> delegate,
                ZxidTracker tracker) {
            return new ZxidTrackingDecorator(delegate, tracker);
        }
        
        private final ZxidTracker tracker;
        private final Factory<? extends ClientCodecConnection> delegate;
        
        private ZxidTrackingDecorator(Factory<? extends ClientCodecConnection> delegate,
                ZxidTracker tracker) {
            this.delegate = delegate;
            this.tracker = tracker;
        }
        
        @Override
        public ClientCodecConnection get() {
            ClientCodecConnection client = delegate.get();
            client.register(tracker);
            return client;
        }
    }
    
    public static ServerViewFactory newInstance(
            ClientConnectionFactory connections,
            ParameterizedFactory<Connection, ? extends ClientCodecConnection> codecFactory,
            ServerQuorumView view,
            TimeValue timeOut) {
        SocketAddress address = view.asNetView().get();
        Factory<Connection> connectionFactory = FixedClientConnectionFactory.newInstance(
                address, connections);
        ZxidTracker tracker = ZxidTracker.create();
        Factory<? extends ClientCodecConnection> codecConnectionFactory = 
                ZxidTrackingDecorator.newInstance(ClientCodecConnection.factory(connectionFactory, codecFactory), tracker);
        return new ServerViewFactory(codecConnectionFactory, timeOut, tracker);
    }

    protected ServerViewFactory(
            Factory<? extends ClientCodecConnection> codecConnectionFactory,
            TimeValue timeOut,
            Reference<Long> lastZxid) {
        super(codecConnectionFactory, timeOut, lastZxid);   
    }
}
