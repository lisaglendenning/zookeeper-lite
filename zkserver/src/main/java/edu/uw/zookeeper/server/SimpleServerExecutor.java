package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.ZNodeNode;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.SessionExecutor;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;

public class SimpleServerExecutor<T extends SessionExecutor> implements ServerExecutor<T> {

    public static Builder builder() {
        return Builder.defaults();
    }

    public static class Builder implements ZooKeeperApplication.RuntimeBuilder<SimpleServerExecutor<?>, Builder> {

        public static Builder defaults() {
            return new Builder(ServerBuilder.defaults());
        }
        
        protected final ServerBuilder server;
        
        protected Builder(ServerBuilder server) {
            this.server = checkNotNull(server);
        }

        public ServerBuilder getServer() {
            return server;
        }

        public Builder setServer(ServerBuilder server) {
            return newInstance(server);
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return server.getRuntimeModule();
        }

        @Override
        public Builder setRuntimeModule(RuntimeModule runtime) {
            return newInstance(server.setRuntimeModule(runtime));
        }

        @Override
        public Builder setDefaults() {
            ServerBuilder server = this.server.setDefaults();
            if (server != this.server) {
                return setServer(server).setDefaults();
            }
            return this;
        }
        
        @Override
        public SimpleServerExecutor<?> build() {
            return setDefaults().doBuild();
        }

        protected Builder newInstance(ServerBuilder server) {
            return new Builder(server);
        }

        protected SimpleServerExecutor<?> doBuild() {
            getServer().build();
            return new SimpleServerExecutor<SimpleSessionExecutor>(
                    getServer().getSessionExecutors(),
                    (SimpleConnectExecutor<?>) getServer().getSessions(),
                    getDefaultAnonymousExecutor());
        }
        
        protected TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> getDefaultAnonymousExecutor() {
            return ProcessorTaskExecutor.of(FourLetterRequestProcessor.newInstance());
        }
    }
    
    public static class ServerBuilder extends SimpleServer.Builder<ServerBuilder> {

        public static ServerBuilder defaults() {
            return new ServerBuilder(new SimpleServerSupplier(null), null, null, null, null, null, null);
        }
        
        protected static class SimpleServerSupplier implements Supplier<SimpleServer> {

            private volatile SimpleServer instance;
            
            public SimpleServerSupplier(SimpleServer instance) {
                this.instance = instance;
            }
            
            public SimpleServerSupplier set(SimpleServer instance) {
                this.instance = instance;
                return this;
            }
            
            @Override
            public SimpleServer get() {
                return instance;
            }
        }
        
        protected final ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors;
        protected final SimpleServerSupplier server;
        
        protected ServerBuilder(
                SimpleServerSupplier server,
                ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors,
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            super(zxids, data, sessions, listeners, runtime);
            this.server = checkNotNull(server);
            this.sessionExecutors = sessionExecutors;
        }

        public ConcurrentMap<Long, SimpleSessionExecutor> getSessionExecutors() {
            return sessionExecutors;
        }

        public ServerBuilder setSessionExecutors(ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors) {
            return newInstance(server, sessionExecutors, zxids, data, sessions, listeners, runtime);
        }

        public ConcurrentMap<Long, SimpleSessionExecutor> getDefaultSessionExecutors() {
            return new MapMaker().makeMap();
        }

        @Override
        public ServerBuilder setDefaults() {
            if (getSessionExecutors() == null) {
                return setSessionExecutors(getDefaultSessionExecutors()).setDefaults();
            }
            return super.setDefaults();
        }

        @Override
        protected ServerBuilder newInstance(
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            return newInstance(server, sessionExecutors, zxids, data, sessions, listeners, runtime);
        }

        protected ServerBuilder newInstance(
                SimpleServerSupplier server,
                ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors,
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            return new ServerBuilder(server, sessionExecutors, zxids, data, sessions, listeners, runtime);
        }

        @Override
        protected SimpleServer doBuild() {
            synchronized (server) {
                if (server.get() == null) {
                    server.set(super.doBuild());
                }
            }
            return server.get();
        }
        
        @Override
        protected Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> getDefaultListeners() {
            return Functions.forMap(getSessionExecutors());
        }

        @Override
        protected SimpleConnectExecutor<?> getDefaultSessions() {
            return SimpleConnectExecutor.defaults(
                    getSessionExecutors(), 
                    getDefaultSessionFactory(), 
                    getRuntimeModule().getConfiguration(), 
                    getZxids());
        }
        
        protected ParameterizedFactory<Session, SimpleSessionExecutor> getDefaultSessionFactory() {
            return SimpleSessionExecutor.factory(
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class), 
                    server);
        }
    }

    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor;
    protected final TaskExecutor<ConnectMessage.Request, ? extends ConnectMessage.Response> connectExecutor;
    protected final ConcurrentMap<Long, T> sessionExecutors;
    
    public SimpleServerExecutor(
            ConcurrentMap<Long, T> sessionExecutors,
            TaskExecutor<ConnectMessage.Request, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor) {
        this.anonymousExecutor = anonymousExecutor;
        this.connectExecutor = connectExecutor;
        this.sessionExecutors = sessionExecutors;
    }
    
    @Override
    public TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor() {
        return anonymousExecutor;
    }

    @Override
    public TaskExecutor<ConnectMessage.Request, ? extends ConnectMessage.Response> connectExecutor() {
        return connectExecutor;
    }

    @Override
    public T sessionExecutor(long sessionId) {
        return sessionExecutors.get(sessionId);
    }

    @Override
    public Iterator<T> iterator() {
        return sessionExecutors.values().iterator();
    }

    public static class ProcessorTaskExecutor<I,O> implements TaskExecutor<I,O> {

        public static <I,O> ProcessorTaskExecutor<I,O> of(
                Processor<? super I, ? extends O> processor) {
            return new ProcessorTaskExecutor<I,O>(processor);
        }
        
        protected final Processor<? super I, ? extends O> processor;
        
        public ProcessorTaskExecutor(Processor<? super I, ? extends O> processor) {
            this.processor = processor;
        }
        
        @Override
        public ListenableFuture<O> submit(I request) {
            try {
                return Futures.immediateFuture((O) processor.apply(request));
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
        }
    }
    
    public static class SimpleConnectExecutor<T extends SessionExecutor> extends AbstractConnectExecutor {
        
        public static <T extends SessionExecutor> SimpleConnectExecutor<T> defaults(
                ConcurrentMap<Long, T> sessions,
                ParameterizedFactory<? super Session, ? extends T> sessionFactory,
                Configuration configuration,
                ZxidReference lastZxid) {
            DefaultSessionParametersPolicy policy = DefaultSessionParametersPolicy.fromConfiguration(configuration);
            return new SimpleConnectExecutor<T>(sessions, sessionFactory, policy, lastZxid);
        }
        
        protected final ConcurrentMap<Long, T> sessions;
        protected final ParameterizedFactory<? super Session, ? extends T> sessionFactory;
        
        public SimpleConnectExecutor(
                ConcurrentMap<Long, T> sessions,
                ParameterizedFactory<? super Session, ? extends T> sessionFactory,
                SessionParametersPolicy policy,
                ZxidReference lastZxid) {
            super(policy, lastZxid);
            this.sessions = sessions;
            this.sessionFactory = sessionFactory;
        }

        @Override
        public ListenableFuture<ConnectMessage.Response> submit(ConnectMessage.Request request) {
            try {
                ConnectMessage.Response response = processor.apply(request);
                if (response instanceof ConnectMessage.Response.Valid) {
                    Session session = response.toSession();
                    T executor = sessionFactory.get(session);
                    T prev = sessions.put(response.getSessionId(), executor);
                    if (prev == null) {
                        logger.debug("Created session: {}", session);
                    } else {
                        logger.debug("Updating session: {} to {}", session, prev.session());
                        // FIXME
                        // what to do?
                        throw new UnsupportedOperationException(String.valueOf(request));
                    }
                }
                return Futures.immediateFuture(response);
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
        }

        @Override
        protected Session put(Session session) {
            // we do the actual session creation in submit()
            return get(session.id());
        }

        @Override
        protected ConcurrentMap<Long, ? extends T> sessions() {
            return sessions;
        }
    }
    
    public static class SimpleSessionExecutor extends AbstractSessionExecutor<Object> {

        public static ParameterizedFactory<Session, SimpleSessionExecutor> factory(
                final ScheduledExecutorService scheduler,
                final Supplier<? extends TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>>> server) {
            checkNotNull(scheduler);
            checkNotNull(server);
            return new ParameterizedFactory<Session, SimpleSessionExecutor>() {
                @Override
                public SimpleSessionExecutor get(Session value) {
                    return new SimpleSessionExecutor(
                            value, 
                            Automatons.createSynchronized(
                                    Automatons.createSimple(
                                            ProtocolState.CONNECTED)),
                            new StrongConcurrentSet<SessionListener>(), 
                            scheduler, 
                            server.get());
                }
            };
        }

        protected final TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server;
        
        public SimpleSessionExecutor(
                Session session,
                Automaton<ProtocolState,ProtocolState> state,
                IConcurrentSet<SessionListener> listeners,
                ScheduledExecutorService scheduler,
                TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server) {
            super(session, state, listeners, scheduler);
            this.server = checkNotNull(server);
        }
        
        @Override
        public ListenableFuture<Message.ServerResponse<?>> submit(Message.ClientRequest<?> request) {
            timer.send(request);
            return server.submit(SessionRequest.of(session.id(), request));
        }

        @Override
        public void onSuccess(Object result) {
        }
    }
}
