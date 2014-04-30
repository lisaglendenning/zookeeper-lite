package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkState;

import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automaton.Transition;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.ProtocolResponse;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public class ConnectionClientExecutorService<I extends Operation.Request, V extends Message.ServerResponse<?>> extends AbstractIdleService 
        implements Supplier<ListenableFuture<ConnectionClientExecutor<I,V,SessionListener,?>>>,
        ClientExecutor<I,V,SessionListener>,
        FutureCallback<ConnectionClientExecutor<I,V,SessionListener,?>>,
        SessionListener {

    public static <I extends Operation.Request, V extends Message.ServerResponse<?>> ConnectionClientExecutorService<I,V> newInstance(
            EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends ConnectionClientExecutor<I,V,SessionListener,?>>> factory) {
        return new ConnectionClientExecutorService<I,V>(factory, 
                new StrongConcurrentSet<SessionListener>(), 
                SameThreadExecutor.getInstance());
    }

    public static <I extends Operation.Request, V extends Message.ServerResponse<?>> V disconnect(ConnectionClientExecutor<I,V,?,?> client) throws InterruptedException, ExecutionException, TimeoutException, KeeperException {
        V response = null;
        if (((client.connection().codec().state().compareTo(ProtocolState.CONNECTED)) <= 0) && 
                (client.connection().state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
            @SuppressWarnings("unchecked")
            ListenableFuture<V> future = client.submit(
                   (I) ProtocolRequestMessage.of(0, Operations.Requests.disconnect().build()));
            int timeOut = client.session().isDone() ? client.session().get().getTimeOut() : 0;
            if (timeOut > 0) {
                response = future.get(timeOut, TimeUnit.MILLISECONDS);
            } else {
                response = future.get();
            }
            Operations.unlessError(response.record());
        }
        return response;
    }

    public static Builder builder() {
        return new Builder(null, null, null, null);
    }
    
    public static class Builder implements ZooKeeperApplication.RuntimeBuilder<List<Service>, Builder> {

        protected final RuntimeModule runtime;
        protected final ClientConnectionFactoryBuilder connectionBuilder;
        protected final ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory;
        protected final ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor;
        
        protected Builder(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
                ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor,
                RuntimeModule runtime) {
            this.runtime = runtime;
            this.connectionBuilder = connectionBuilder;
            this.clientConnectionFactory = clientConnectionFactory;
            this.clientExecutor = clientExecutor;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @Override
        public Builder setRuntimeModule(RuntimeModule runtime) {
            if (this.runtime == runtime) {
                return this;
            } else {
                return newInstance(
                        (connectionBuilder == null) ? connectionBuilder : connectionBuilder.setRuntimeModule(runtime), 
                        clientConnectionFactory, 
                        clientExecutor,
                        runtime);
            }
        }
        
        public ClientConnectionFactoryBuilder getConnectionBuilder() {
            return connectionBuilder;
        }

        public Builder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
            if (this.connectionBuilder == connectionBuilder) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
            }
        }
        
        public ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> getClientConnectionFactory() {
            return clientConnectionFactory;
        }

        public Builder setClientConnectionFactory(
                ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory) {
            if (this.clientConnectionFactory == clientConnectionFactory) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
            }
        }
        
        public ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> getConnectionClientExecutor() {
            return clientExecutor;
        }

        public Builder setConnectionClientExecutor(
                ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor) {
            if (this.clientExecutor == clientExecutor) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
            }
        }

        @Override
        public Builder setDefaults() {
            checkState(getRuntimeModule() != null);
        
            if (this.connectionBuilder == null) {
                return setConnectionBuilder(getDefaultClientConnectionFactoryBuilder()).setDefaults();
            }
            ClientConnectionFactoryBuilder connectionBuilder = this.connectionBuilder.setDefaults();
            if (this.connectionBuilder != connectionBuilder) {
                return setConnectionBuilder(connectionBuilder).setDefaults();
            }
            if (clientConnectionFactory == null) {
                return setClientConnectionFactory(getDefaultClientConnectionFactory()).setDefaults();
            }
            if (clientExecutor == null) {
                return setConnectionClientExecutor(getDefaultConnectionClientExecutorService()).setDefaults();
            }
            return this;
        }

        @Override
        public List<Service> build() {
            return setDefaults().getServices();
        }
        
        protected Builder newInstance(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
                ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor,
                RuntimeModule runtime) {
            return new Builder(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        }
        
        protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
            return ClientConnectionFactoryBuilder.defaults().setRuntimeModule(getRuntimeModule()).setDefaults();
        }
        
        protected ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> getDefaultClientConnectionFactory() {
            return getConnectionBuilder().build();
        }
        
        protected EnsembleView<ServerInetAddressView> getDefaultEnsemble() {
            return ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
        }

        protected ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> getDefaultConnectionClientExecutorService() {
            EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<?>>> ensembleFactory = 
                    EnsembleViewFactory.fromSession(
                        getClientConnectionFactory(),
                        getDefaultEnsemble(), 
                        getConnectionBuilder().getTimeOut(),
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
            ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> service =
                    ConnectionClientExecutorService.newInstance(
                            ensembleFactory);
            return service;
        }

        protected List<Service> getServices() {
            return Lists.<Service>newArrayList(
                    getClientConnectionFactory(), 
                    getConnectionClientExecutor());
        }   
    }
    
    protected final Logger logger;
    protected final Executor executor;
    protected final EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends ConnectionClientExecutor<I,V,SessionListener,?>>> factory;
    protected final IConcurrentSet<SessionListener> listeners;
    protected final Client client;
    
    protected ConnectionClientExecutorService(
            EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends ConnectionClientExecutor<I,V,SessionListener,?>>> factory,
            IConcurrentSet<SessionListener> listeners,
            Executor executor) {
        this.logger = LogManager.getLogger(getClass());
        this.factory = factory;
        this.listeners = listeners;
        this.executor = executor;
        this.client = new Client();
    }

    @Override
    public ListenableFuture<ConnectionClientExecutor<I,V,SessionListener,?>> get() {
        return client;
    }

    @Override
    public ListenableFuture<V> submit(I request) {
        try {
            return get().get().submit(request);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<V> submit(I request, Promise<V> promise) {
        try {
            return get().get().submit(request, promise);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public synchronized void subscribe(SessionListener listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized boolean unsubscribe(SessionListener listener) {
        return listeners.remove(listener);
    }

    @Override
    public void handleAutomatonTransition(Transition<ProtocolState> transition) {
        for (SessionListener listener: listeners) {
            listener.handleAutomatonTransition(transition);
        }
    }

    @Override
    public void handleNotification(ProtocolResponse<IWatcherEvent> notification) {
        for (SessionListener listener: listeners) {
            listener.handleNotification(notification);
        }
    }

    @Override
    public synchronized void onSuccess(ConnectionClientExecutor<I,V,SessionListener,?> result) {
        result.subscribe(this);
    }

    @Override
    public void onFailure(Throwable t) {
        // TODO
        logger.debug("{}", this, t);
        stopAsync();
    }

    @Override
    protected void startUp() throws Exception {
        client.run();
        client.get();
    }
    
    @Override
    protected void shutDown() throws Exception {
        if (client.isDone()) {
            ConnectionClientExecutor<I,V,SessionListener,?> instance = null;
            try { 
                instance = client.get();
                disconnect(instance);      
            } finally {
                try {
                    if (instance != null) {
                        instance.connection().close();
                        instance.unsubscribe(this);
                    }
                } catch (Exception e) {}
            } 
        } else {
            client.cancel(true);
        }
        
        Iterator<?> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            itr.next();
        }
    }
    
    /**
     * Opens a new connection to a different server if the current connection drops.
     */
    protected class Client extends ForwardingPromise<ConnectionClientExecutor<I,V,SessionListener,?>> implements Runnable, Connection.Listener<Object> {

        protected volatile ServerInetAddressView server;
        protected volatile ListenableFuture<? extends ConnectionClientExecutor<I,V,SessionListener,?>> future;
        protected volatile Promise<ConnectionClientExecutor<I,V,SessionListener,?>> promise;
        
        public Client() {
            this.server = null;
            this.future = null;
            this.promise = newPromise();
        }
        
        protected Promise<ConnectionClientExecutor<I,V,SessionListener,?>> newPromise() {
            Promise<ConnectionClientExecutor<I,V,SessionListener,?>> promise =
                    LoggingPromise.create(logger, 
                            SettableFuturePromise.<ConnectionClientExecutor<I,V,SessionListener,?>>create());
            Futures.addCallback(promise, ConnectionClientExecutorService.this);
            promise.addListener(this, SameThreadExecutor.getInstance());
            return promise;
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            if (future != null) {
                future.cancel(mayInterruptIfRunning);
            }
            return super.cancel(mayInterruptIfRunning);
        }

        @Override
        public synchronized boolean set(ConnectionClientExecutor<I,V,SessionListener,?> value) {
            if (! isDone()) {
                value.connection().subscribe(this);
                return super.set(value);
            }
            return false;
        }
        
        @Override
        public synchronized void run() {
            switch (state()) {
            case STOPPING:
            case TERMINATED:
            case FAILED:
                cancel(true);
                return;
            default:
                break;
            }
            
            if (future == null) {
                if (! isCancelled()) {
                    if (server == null) {
                        server = factory.select();
                    }
                    if (isDone()) {
                        try {
                            if (get().session().isDone()) {
                                ConnectMessage.Response response = get().session().get();
                                if (response instanceof ConnectMessage.Response.Valid) {
                                    backoff();
                                    Session session = response.toSession();
                                    logger.info("Reconnecting session {} to {}", session, server);
                                    future = factory.get(server).get(session);
                                }
                            }
                        } catch (Exception e) {
                            future = null;
                        }
                        promise = newPromise();
                    }
                    if (future == null) {
                        logger.info("Connecting new session to {}", server);
                        future = factory.get(server).get();
                    }
                    future.addListener(this, executor);
                }
            } else if (future.isDone()) {
                if (! isDone()) {
                    if (future.isCancelled()) {
                        cancel(true);
                    } else {
                        try {
                            ConnectionClientExecutor<I,V,SessionListener,?> connection = future.get();
                            if (connection.session().isDone()) {
                                ConnectMessage.Response session;
                                try {
                                    session = connection.session().get();
                                } catch (CancellationException e) {
                                    throw new ExecutionException(e);
                                }
                                if (session instanceof ConnectMessage.Response.Valid) {
                                    set(connection);
                                    return;
                                } else {
                                    // try again with a new session
                                    future = null;
                                    run();
                                    return;
                                }
                            } else {
                                connection.session().addListener(this, executor);
                                return;
                            }
                        } catch (ExecutionException e) {
                            logger.warn("Error connecting to {}", server, e.getCause());
                            if (factory.view().size() > 1) {
                                ServerInetAddressView prevServer = server;
                                do {
                                    server = factory.select();
                                } while (server.equals(prevServer));
                                future = null;
                                run();
                                return;
                            } else {
                                setException(e.getCause());
                            }
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            }
        }

        @Override
        public synchronized void handleConnectionState(Automaton.Transition<Connection.State> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                ConnectionClientExecutor<I, V, SessionListener, ?> instance;
                try {
                    instance = get();
                } catch (Exception e) {
                    return;
                }
                if (instance != null) {
                    instance.connection().unsubscribe(this);
                    instance.unsubscribe(ConnectionClientExecutorService.this);
                }
                if (isRunning()) {
                    logger.warn("Connection closed to {}", server);
                    if (factory.view().size() > 1) {
                        ServerInetAddressView prevServer = server;
                        do {
                            server = factory.select();
                        } while (server.equals(prevServer));
                        future = null;
                        run();
                        return;
                    } else {
                        setException(new ClosedChannelException());
                    }
                }
            }
        }

        @Override
        public void handleConnectionRead(Object message) {
        }
        
        protected void backoff() throws InterruptedException {
            // it seems that when the ensemble is undergoing election
            // that connections may be refused
            // so we'll wait a little while before trying to connect
            Random random = new Random();
            int millis = random.nextInt(1000) + 1000;
            Thread.sleep(millis);
        }

        @Override
        protected Promise<ConnectionClientExecutor<I,V,SessionListener,?>> delegate() {
            return promise;
        }
    }
}
