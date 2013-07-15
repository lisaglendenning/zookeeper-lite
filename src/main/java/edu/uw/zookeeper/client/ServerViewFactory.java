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

public class ServerViewFactory<V, T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> extends Pair<T, ZxidTracker> implements DefaultsFactory<V, ClientConnectionExecutor<C>> {

    public static <T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactory<Session, T, C> newInstance(
            ClientConnectionFactory<?, C> connections,
            T view,
            TimeValue timeOut) {
        ZxidTracker zxids = ZxidTracker.create();
        final DefaultsFactory<Session, ConnectMessage.Request> requestFactory = ConnectMessage.Request.factory(timeOut, zxids);
        final ParameterizedFactory<ConnectMessage.Request, ClientConnectionExecutor<C>> delegate = 
                FromRequestFactory.create(
                        FixedClientConnectionFactory.create(view.get(), connections));
        return newInstance(
                view,
                new DefaultsFactory<Session, ClientConnectionExecutor<C>>() {
                    @Override
                    public ClientConnectionExecutor<C> get() {
                        return delegate.get(requestFactory.get());
                    }

                    @Override
                    public ClientConnectionExecutor<C> get(Session value) {
                        return delegate.get(requestFactory.get(value));
                    }
                }, 
                zxids);
    }
    
    public static <V, T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactory<V,T,C> newInstance(
            T view,
            DefaultsFactory<V, ClientConnectionExecutor<C>> delegate,
            ZxidTracker zxids) {
        return new ServerViewFactory<V,T,C>(view, delegate, zxids);
    }
    
    public static class FixedClientConnectionFactory<C extends Connection<?>> extends Pair<SocketAddress, ClientConnectionFactory<?,C>> implements Factory<C> {
        
        public static <C extends Connection<?>> FixedClientConnectionFactory<C> create(
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

    public static class FromRequestFactory<C extends Connection<? super Message.ClientSession>> implements DefaultsFactory<ConnectMessage.Request, ClientConnectionExecutor<C>> {
    
        public static <C extends Connection<? super Message.ClientSession>> FromRequestFactory<C> create(
                Factory<C> connections) {
            return new FromRequestFactory<C>(connections);
        }
        
        protected final Factory<C> connections;
        
        public FromRequestFactory(
                Factory<C> connections) {
            this.connections = connections;
        }

        @Override
        public ClientConnectionExecutor<C> get() {
            return get(ConnectMessage.Request.NewRequest.newInstance());
        }
        
        @Override
        public ClientConnectionExecutor<C> get(ConnectMessage.Request request) {
            C connection;
            try {
                connection = connections.get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            ClientConnectionExecutor<C> instance = 
                    ClientConnectionExecutor.newInstance(
                            request, connection);
            return instance;
        }
    }

    protected final DefaultsFactory<V,ClientConnectionExecutor<C>> delegate;
    
    protected ServerViewFactory(
            T view,
            DefaultsFactory<V, ClientConnectionExecutor<C>> delegate,
            ZxidTracker zxids) {
        super(view, zxids);
        this.delegate = delegate;
    }
    
    @Override
    public ClientConnectionExecutor<C> get() {
        ClientConnectionExecutor<C> instance =  delegate.get();
        ZxidTracker.ZxidListener.create(second(), instance.get());
        return instance;
    }

    @Override
    public ClientConnectionExecutor<C> get(V value) {
        ClientConnectionExecutor<C> instance = delegate.get(value);
        ZxidTracker.ZxidListener.create(second(), instance.get());
        return instance;
    }
}
