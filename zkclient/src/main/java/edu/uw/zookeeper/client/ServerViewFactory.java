package edu.uw.zookeeper.client;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;

public class ServerViewFactory<V, C extends AbstractConnectionClientExecutor<?,?,?>> extends Pair<ServerInetAddressView, ZxidTracker> implements DefaultsFactory<V, ListenableFuture<C>>, Function<C, C> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ServerViewFactory<Session, OperationClientExecutor<C>> newInstance(
            ClientConnectionFactory<C> connections,
            ServerInetAddressView view,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        ZxidTracker zxids = ZxidTracker.create();
        final DefaultsFactory<Session, ConnectMessage.Request> requestFactory = ConnectMessage.Request.factory(timeOut, zxids);
        final FromRequestFactory<C> delegate = 
                FromRequestFactory.create(
                        FixedClientConnectionFactory.create(view.get(), connections),
                        executor);
        return new ServerViewFactory<Session, OperationClientExecutor<C>>(
                view,
                new DefaultsFactory<Session, ListenableFuture<? extends OperationClientExecutor<C>>>() {
                    @Override
                    public ListenableFuture<OperationClientExecutor<C>> get() {
                        return delegate.get(requestFactory.get());
                    }

                    @Override
                    public ListenableFuture<OperationClientExecutor<C>> get(Session value) {
                        return delegate.get(requestFactory.get(value));
                    }
                }, 
                zxids);
    }
    
    public static <V, C extends AbstractConnectionClientExecutor<?,?,?>> ServerViewFactory<V,C> newInstance(
            ServerInetAddressView view,
            DefaultsFactory<V, ? extends ListenableFuture<? extends C>> delegate,
            ZxidTracker zxids) {
        return new ServerViewFactory<V,C>(view, delegate, zxids);
    }
    
    public static class FromRequestFactory<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> implements DefaultsFactory<ConnectMessage.Request, ListenableFuture<OperationClientExecutor<C>>> {
    
        public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> FromRequestFactory<C> create(
                Factory<ListenableFuture<C>> connections,
                ScheduledExecutorService executor) {
            return new FromRequestFactory<C>(connections, executor);
        }
        
        protected final Factory<ListenableFuture<C>> connections;
        protected final ScheduledExecutorService executor;
        
        public FromRequestFactory(
                Factory<ListenableFuture<C>> connections,
                ScheduledExecutorService executor) {
            this.connections = connections;
            this.executor = executor;
        }

        @Override
        public ListenableFuture<OperationClientExecutor<C>> get() {
            return get(ConnectMessage.Request.NewRequest.newInstance());
        }
        
        @Override
        public ListenableFuture<OperationClientExecutor<C>> get(ConnectMessage.Request request) {
            return Futures.transform(connections.get(), new Constructor(request), sameThreadExecutor);
        }
        
        protected class Constructor implements Function<C, OperationClientExecutor<C>> {

            protected final ConnectMessage.Request task;
            
            public Constructor(ConnectMessage.Request task) {
                this.task = task;
            }
            
            @Override
            public OperationClientExecutor<C> apply(C input) {
                return OperationClientExecutor.newInstance(
                        task, input, executor);
            }
        }
    }
    
    protected final static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

    protected final DefaultsFactory<V, ? extends ListenableFuture<? extends C>> delegate;
    
    protected ServerViewFactory(
            ServerInetAddressView view,
            DefaultsFactory<V, ? extends ListenableFuture<? extends C>> delegate,
            ZxidTracker zxids) {
        super(view, zxids);
        this.delegate = delegate;
    }
    
    @Override
    public ListenableFuture<C> get() {
        return Futures.transform(delegate.get(), this, sameThreadExecutor);
    }

    @Override
    public ListenableFuture<C> get(V value) {
        return Futures.transform(delegate.get(value), this, sameThreadExecutor);
    }

    @Override
    public C apply(C input) {
        ZxidTracker.ZxidListener.create(second(), input.get());
        return input;
    }
}
