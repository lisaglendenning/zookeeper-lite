package edu.uw.zookeeper.client;

import java.net.SocketAddress;
import com.google.common.base.Throwables;

import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.TimeValue;

public class ServerViewFactory<T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> implements DefaultsFactory<Session, ClientConnectionExecutor<C>> {

    public static <T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactory<T,C> newInstance(
            ClientConnectionFactory<?, C> connections,
            T server,
            TimeValue timeOut) {
        ZxidTracker zxids = ZxidTracker.create();
        DefaultsFactory<Session, ConnectMessage.Request> requestFactory = ConnectMessage.Request.factory(timeOut, zxids);
        return new ServerViewFactory<T,C>(requestFactory, zxids, FromRequestFactory.newInstance(server, connections));
    }
    
    public static class FromRequestFactory<T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> extends Pair<T, ClientConnectionFactory<?, C>> implements ParameterizedFactory<ConnectMessage.Request, ClientConnectionExecutor<C>> {
    
        public static <T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> FromRequestFactory<T,C> newInstance(
                T server,
                ClientConnectionFactory<?, C> connections) {
            return new FromRequestFactory<T,C>(server, connections);
        }
        
        public FromRequestFactory(
                T server,
                ClientConnectionFactory<?, C> connections) {
            super(server, connections);
        }
        
        @Override
        public ClientConnectionExecutor<C> get(ConnectMessage.Request request) {
            C connection;
            try {
                connection = second().connect(first().get()).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            ClientConnectionExecutor<C> instance = 
                    ClientConnectionExecutor.newInstance(
                            request, connection);
            return instance;
        }
    }

    public static class FixedClientConnectionFactory<C extends Connection<?>> extends Pair<SocketAddress, ClientConnectionFactory<?,C>> implements Factory<C> {
        
        public static <C extends Connection<?>> FixedClientConnectionFactory<C> newInstance(
                SocketAddress address,
                ClientConnectionFactory<?,C> connectionFactory) {
            return new FixedClientConnectionFactory<C>(address, connectionFactory);
        }
        
        protected FixedClientConnectionFactory(SocketAddress address,
                ClientConnectionFactory<?,C> connectionFactory) {
            super(address, connectionFactory);
        }
        
        @Override
        public C get() {
            try {
                return second().connect(first()).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    protected final DefaultsFactory<Session, ConnectMessage.Request> requestFactory;
    protected final ZxidTracker zxids;
    protected final FromRequestFactory<T,C> delegate;
    
    protected ServerViewFactory(
            DefaultsFactory<Session, ConnectMessage.Request> requestFactory,
            ZxidTracker zxids,
            FromRequestFactory<T,C> delegate) {
        this.zxids = zxids;
        this.requestFactory = requestFactory;
        this.delegate = delegate;
    }
    
    public T address() {
        return delegate.first();
    }
    
    public ClientConnectionFactory<?,C> connections() {
        return delegate.second();
    }

    public ZxidTracker zxids() {
        return zxids;
    }

    @Override
    public ClientConnectionExecutor<C> get() {
        ClientConnectionExecutor<C> instance =  delegate.get(requestFactory.get());
        ZxidTracker.ZxidListener.create(zxids(), instance.get());
        return instance;
    }

    @Override
    public ClientConnectionExecutor<C> get(Session session) {
        ClientConnectionExecutor<C> instance = delegate.get(requestFactory.get(session));
        ZxidTracker.ZxidListener.create(zxids(), instance.get());
        return instance;
    }
}
