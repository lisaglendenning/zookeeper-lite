package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkState;

import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;

public class ConnectionClientExecutorService<I extends Operation.Request, V extends Message.ServerResponse<?>> extends AbstractIdleService 
        implements Supplier<ListenableFuture<ConnectionClientExecutor<I,V,?>>>, 
        Publisher, 
        ClientExecutor<I, V>,
        FutureCallback<ConnectionClientExecutor<I,V,?>> {

    public static <I extends Operation.Request, V extends Message.ServerResponse<?>> ConnectionClientExecutorService<I,V> newInstance(
            EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends ConnectionClientExecutor<I,V,?>>> factory) {
        return new ConnectionClientExecutorService<I,V>(factory);
    }
    
    public static <I extends Operation.Request, V extends Message.ServerResponse<?>> V disconnect(ConnectionClientExecutor<I,V,?> client) throws InterruptedException, ExecutionException, TimeoutException, KeeperException {
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
        protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory;
        protected final ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor;
        
        protected Builder(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
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
        
        public ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getClientConnectionFactory() {
            return clientConnectionFactory;
        }

        public Builder setClientConnectionFactory(
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory) {
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
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> clientExecutor,
                RuntimeModule runtime) {
            return new Builder(connectionBuilder, clientConnectionFactory, clientExecutor, runtime);
        }
        
        protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
            return ClientConnectionFactoryBuilder.defaults().setRuntimeModule(getRuntimeModule()).setDefaults();
        }
        
        protected ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getDefaultClientConnectionFactory() {
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
    protected final EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends ConnectionClientExecutor<I,V,?>>> factory;
    protected final Set<Object> handlers;
    protected final Queue<Object> events;
    protected final Client client;
    
    protected ConnectionClientExecutorService(
            EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends ConnectionClientExecutor<I,V,?>>> factory) {
        this.logger = LogManager.getLogger(getClass());
        this.factory = factory;
        this.handlers = Collections.synchronizedSet(Sets.newHashSet());
        this.events = Queues.newConcurrentLinkedQueue();
        this.executor = MoreExecutors.sameThreadExecutor();
        this.client = new Client();
    }

    @Override
    public ListenableFuture<ConnectionClientExecutor<I,V,?>> get() {
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
    public synchronized void post(Object event) {
        if (!client.isDone() || !events.isEmpty()) {
            events.add(event);
        } else {
            ConnectionClientExecutor<I,V,?> client;
            try {
                client = this.client.get(0, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                events.add(event);
                return;
            }
            client.post(event);
        }
    }

    @Override
    public synchronized void register(Object handler) {
        handlers.add(handler);
        if (client.isDone()) {
            try {
                client.get(0, TimeUnit.MILLISECONDS).register(handler);
            } catch (Exception e) {
            }
        }
    }

    @Override
    public synchronized void unregister(Object handler) {
        handlers.remove(handler);
        if (client.isDone()) {
            try {
                client.get(0, TimeUnit.MILLISECONDS).unregister(handler);
            } catch (Exception e) {
            }
        }
    }

    @Override
    public synchronized void onSuccess(ConnectionClientExecutor<I,V,?> result) {
        try {
            synchronized (handlers) {
                for (Object handler: handlers) {
                    result.register(handler);
                }
            }
    
            Object event;
            while ((event = events.poll()) != null) {
                result.post(event);
            }
        } catch (Exception e) {
            // TODO
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        // TODO
        stopAsync();
    }

    @Override
    protected void startUp() throws Exception {
        client.run();
        client.get();
    }
    
    @Override
    protected synchronized void shutDown() throws Exception {
        if (client.isDone()) {
            try {
                    disconnect(client.get());      
            } finally {
                try {
                    client.get().connection().close();
                } catch (Exception e) {}
            } 
        } else {
            client.cancel(true);
        }
        handlers.clear();
        events.clear();
    }
    
    /**
     * Opens a new connection to a different server if the current connection drops.
     */
    protected class Client extends ForwardingPromise<ConnectionClientExecutor<I,V,?>> implements Runnable {

        protected volatile ServerInetAddressView server;
        protected volatile ListenableFuture<? extends ConnectionClientExecutor<I,V,?>> future;
        protected volatile Promise<ConnectionClientExecutor<I,V,?>> promise;
        
        public Client() {
            this.server = null;
            this.future = null;
            this.promise = newPromise();
        }
        
        protected Promise<ConnectionClientExecutor<I,V,?>> newPromise() {
            return LoggingPromise.create(logger, 
                    SettableFuturePromise.<ConnectionClientExecutor<I,V,?>>create());
        }
        
        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            if (future != null) {
                future.cancel(mayInterruptIfRunning);
            }
            return super.cancel(mayInterruptIfRunning);
        }

        @Override
        public synchronized boolean set(ConnectionClientExecutor<I,V,?> value) {
            if (! isDone()) {
                new Handler(value);
                ConnectionClientExecutorService.this.onSuccess(value);
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
                return;
            }
            
            if (future.isDone()) {
                if (! isDone()) {
                    if (future.isCancelled()) {
                        cancel(true);
                    } else {
                        try {
                            ConnectionClientExecutor<I,V,?> connection = future.get();
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
                            setException(e);
                        }
                    }
                }
            }
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
        protected Promise<ConnectionClientExecutor<I,V,?>> delegate() {
            return promise;
        }
        
        protected class Handler {

            protected final ConnectionClientExecutor<I,V,?> instance;
            
            public Handler(ConnectionClientExecutor<I,V,?> instance) {
                this.instance = instance;
                instance.register(this);
            }
            
            @Subscribe
            public void handleStateEvent(Automaton.Transition<?> event) {
                if (Connection.State.CONNECTION_CLOSED == event.to()) {
                    synchronized (Client.this) {
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
                    try {
                        instance.unregister(this);
                    } catch (IllegalArgumentException e) {}
                }
            }
        }
    }
}
