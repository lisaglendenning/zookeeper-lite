package edu.uw.zookeeper.client;

import java.net.SocketAddress;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;

public class ServerViewFactory<V, T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> extends Pair<T, ZxidTracker> implements DefaultsFactory<V, ListenableFuture<ClientConnectionExecutor<C>>>, Function<ClientConnectionExecutor<C>, ClientConnectionExecutor<C>> {

    public static <T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactory<Session, T, C> newInstance(
            ClientConnectionFactory<C> connections,
            T view,
            TimeValue timeOut) {
        ZxidTracker zxids = ZxidTracker.create();
        final DefaultsFactory<Session, ConnectMessage.Request> requestFactory = ConnectMessage.Request.factory(timeOut, zxids);
        final ParameterizedFactory<ConnectMessage.Request, ListenableFuture<ClientConnectionExecutor<C>>> delegate = 
                FromRequestFactory.create(
                        FixedClientConnectionFactory.create(view.get(), connections));
        return newInstance(
                view,
                new DefaultsFactory<Session, ListenableFuture<ClientConnectionExecutor<C>>>() {
                    @Override
                    public ListenableFuture<ClientConnectionExecutor<C>> get() {
                        return delegate.get(requestFactory.get());
                    }

                    @Override
                    public ListenableFuture<ClientConnectionExecutor<C>> get(Session value) {
                        return delegate.get(requestFactory.get(value));
                    }
                }, 
                zxids);
    }
    
    public static <V, T extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactory<V,T,C> newInstance(
            T view,
            DefaultsFactory<V, ListenableFuture<ClientConnectionExecutor<C>>> delegate,
            ZxidTracker zxids) {
        return new ServerViewFactory<V,T,C>(view, delegate, zxids);
    }
    
    public static class FixedClientConnectionFactory<C extends Connection<?>> extends Pair<SocketAddress, ClientConnectionFactory<C>> implements Factory<ListenableFuture<C>> {
        
        public static <C extends Connection<?>> FixedClientConnectionFactory<C> create(
                SocketAddress address,
                ClientConnectionFactory<C> connectionFactory) {
            return new FixedClientConnectionFactory<C>(address, connectionFactory);
        }
        
        protected FixedClientConnectionFactory(SocketAddress address,
                ClientConnectionFactory<C> connectionFactory) {
            super(address, connectionFactory);
        }
        
        @Override
        public ListenableFuture<C> get() {
            try {
                return second().connect(first());
            } catch (Throwable t) {
                return Futures.immediateFailedFuture(t);
            }
        }
    }

    public static class FromRequestFactory<C extends Connection<? super Message.ClientSession>> implements DefaultsFactory<ConnectMessage.Request, ListenableFuture<ClientConnectionExecutor<C>>> {
    
        public static <C extends Connection<? super Message.ClientSession>> FromRequestFactory<C> create(
                Factory<ListenableFuture<C>> connections) {
            return new FromRequestFactory<C>(connections);
        }
        
        protected final Factory<ListenableFuture<C>> connections;
        
        public FromRequestFactory(
                Factory<ListenableFuture<C>> connections) {
            this.connections = connections;
        }

        @Override
        public ListenableFuture<ClientConnectionExecutor<C>> get() {
            return get(ConnectMessage.Request.NewRequest.newInstance());
        }
        
        @Override
        public ListenableFuture<ClientConnectionExecutor<C>> get(ConnectMessage.Request request) {
            return Futures.transform(connections.get(), new Constructor(request));
        }
        
        protected class Constructor implements Function<C, ClientConnectionExecutor<C>> {

            protected final ConnectMessage.Request task;
            
            public Constructor(ConnectMessage.Request task) {
                this.task = task;
            }
            
            @Override
            public ClientConnectionExecutor<C> apply(C input) {
                return ClientConnectionExecutor.newInstance(
                        task, input);
            }
        }
    }

    protected final DefaultsFactory<V, ListenableFuture<ClientConnectionExecutor<C>>> delegate;
    
    protected ServerViewFactory(
            T view,
            DefaultsFactory<V, ListenableFuture<ClientConnectionExecutor<C>>> delegate,
            ZxidTracker zxids) {
        super(view, zxids);
        this.delegate = delegate;
    }
    
    @Override
    public ListenableFuture<ClientConnectionExecutor<C>> get() {
        return Futures.transform(delegate.get(), this);
    }

    @Override
    public ListenableFuture<ClientConnectionExecutor<C>> get(V value) {
        return Futures.transform(delegate.get(value), this);
    }

    @Override
    public ClientConnectionExecutor<C> apply(ClientConnectionExecutor<C> input) {
        ZxidTracker.ZxidListener.create(second(), input.get());
        return input;
    }
}
