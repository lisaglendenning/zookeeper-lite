package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.bus.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.ConnectMessage.Request;
import edu.uw.zookeeper.protocol.ConnectMessage.Response;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ServerResponse;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.TimeOutCallback;
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
            return new SimpleServerExecutor<SessionExecutor>(
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
        
        protected final ConcurrentMap<Long, SessionExecutor> sessionExecutors;
        protected final SimpleServerSupplier server;
        
        protected ServerBuilder(
                SimpleServerSupplier server,
                ConcurrentMap<Long, SessionExecutor> sessionExecutors,
                ZxidGenerator zxids,
                ZNodeDataTrie data,
                SessionManager sessions,
                Function<Long, ? extends PubSubSupport<? super ServerResponse<?>>> publishers,
                RuntimeModule runtime) {
            super(zxids, data, sessions, publishers, runtime);
            this.server = checkNotNull(server);
            this.sessionExecutors = sessionExecutors;
        }

        public ConcurrentMap<Long, SessionExecutor> getSessionExecutors() {
            return sessionExecutors;
        }

        public ServerBuilder setSessionExecutors(ConcurrentMap<Long, SessionExecutor> sessionExecutors) {
            return newInstance(server, sessionExecutors, zxids, data, sessions, publishers, runtime);
        }

        public ConcurrentMap<Long, SessionExecutor> getDefaultSessionExecutors() {
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
                ZNodeDataTrie data,
                SessionManager sessions,
                Function<Long, ? extends PubSubSupport<? super ServerResponse<?>>> publishers,
                RuntimeModule runtime) {
            return newInstance(server, sessionExecutors, zxids, data, sessions, publishers, runtime);
        }

        protected ServerBuilder newInstance(
                SimpleServerSupplier server,
                ConcurrentMap<Long, SessionExecutor> sessionExecutors,
                ZxidGenerator zxids,
                ZNodeDataTrie data,
                SessionManager sessions,
                Function<Long, ? extends PubSubSupport<? super ServerResponse<?>>> publishers,
                RuntimeModule runtime) {
            return new ServerBuilder(server, sessionExecutors, zxids, data, sessions, publishers, runtime);
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
        protected Function<Long, ? extends PubSubSupport<? super ServerResponse<?>>> getDefaultPublishers() {
            return new Function<Long, PubSubSupport<? super ServerResponse<?>>>() {
                @Override
                public PubSubSupport<? super ServerResponse<?>> apply(Long input) {
                    return getSessionExecutors().get(input);
                }
            };
        }

        @Override
        protected SimpleConnectExecutor<?> getDefaultSessions() {
            return SimpleConnectExecutor.defaults(
                    getSessionExecutors(), 
                    getDefaultSessionFactory(), 
                    getRuntimeModule().getConfiguration(), 
                    getZxids());
        }
        
        protected ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, SimpleSessionExecutor> getDefaultSessionFactory() {
            return SimpleSessionExecutor.factory(
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class), 
                    server);
        }
    }
    
    protected final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor;
    protected final TaskExecutor<Pair<Request, ? extends PubSubSupport<Object>>, ? extends Response> connectExecutor;
    protected final ConcurrentMap<Long, T> sessionExecutors;
    
    public SimpleServerExecutor(
            ConcurrentMap<Long, T> sessionExecutors,
            TaskExecutor<Pair<Request, ? extends PubSubSupport<Object>>, ? extends Response> connectExecutor,
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
    public TaskExecutor<Pair<Request, ? extends PubSubSupport<Object>>, ? extends Response> connectExecutor() {
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
                ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, ? extends T> sessionFactory,
                Configuration configuration,
                ZxidReference lastZxid) {
            DefaultSessionParametersPolicy policy = DefaultSessionParametersPolicy.fromConfiguration(configuration);
            @SuppressWarnings("rawtypes")
            PubSubSupport<Object> publisher = new SyncMessageBus<Object>(new SyncBusConfiguration());
            return new SimpleConnectExecutor<T>(sessions, sessionFactory, publisher, policy, lastZxid);
        }
        
        protected final ConcurrentMap<Long, T> sessions;
        protected final ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, ? extends T> sessionFactory;
        
        public SimpleConnectExecutor(
                ConcurrentMap<Long, T> sessions,
                ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, ? extends T> sessionFactory,
                PubSubSupport<? super SessionEvent> publisher,
                SessionParametersPolicy policy,
                ZxidReference lastZxid) {
            super(publisher, policy, lastZxid);
            this.sessions = sessions;
            this.sessionFactory = sessionFactory;
        }

        @Override
        public ListenableFuture<ConnectMessage.Response> submit(
                Pair<ConnectMessage.Request, ? extends PubSubSupport<Object>> request) {
            try {
                ConnectMessage.Response response = processor.apply(request.first());
                if (response instanceof ConnectMessage.Response.Valid) {
                    Session session = response.toSession();
                    T executor = sessionFactory.get(Pair.create(session, (PubSubSupport<Object>) request.second()));
                    T prev = sessions.put(response.getSessionId(), executor);
                    if (prev == null) {
                        logger().debug("Created session: {}", session);
                        publish(SessionStateEvent.create(session, Session.State.SESSION_OPENED));
                    } else {
                        logger().debug("Updating session: {} to {}", session, prev.session());
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
        protected ConcurrentMap<Long, ? extends SessionExecutor> sessions() {
            return sessions;
        }
    }
    
    public static class SimpleSessionExecutor implements SessionExecutor, FutureCallback<Object> {

        public static ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, SimpleSessionExecutor> factory(
                final ScheduledExecutorService scheduler,
                final Supplier<? extends TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>>> server) {
            checkNotNull(scheduler);
            checkNotNull(server);
            return new ParameterizedFactory<Pair<Session, PubSubSupport<Object>>, SimpleSessionExecutor>() {
                @Override
                public SimpleSessionExecutor get(
                        Pair<Session, PubSubSupport<Object>> value) {
                    return new SimpleSessionExecutor(
                            value.first(), value.second(), scheduler, server.get());
                }
            };
        }

        protected final PubSubSupport<Object> publisher;
        protected final Session session;
        protected final TimeOutCallback timer;
        protected final TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server;
        
        public SimpleSessionExecutor(
                Session session,
                PubSubSupport<Object> publisher,
                ScheduledExecutorService scheduler,
                TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server) {
            this.session = checkNotNull(session);
            this.publisher = checkNotNull(publisher);
            this.server = checkNotNull(server);
            this.timer = TimeOutCallback.create(
                    TimeOutParameters.create(session.parameters().timeOut()), 
                    scheduler, 
                    this);
        }

        @Override
        public Session session() {
            return session;
        }

        @Override
        public void subscribe(Object listener) {
            publisher.subscribe(listener);
        }

        @Override
        public boolean unsubscribe(Object listener) {
            return publisher.unsubscribe(listener);
        }

        @Override
        public void publish(Object message) {
            publisher.publish(message);
        }
        
        @Override
        public ListenableFuture<Message.ServerResponse<?>> submit(Message.ClientRequest<?> request) {
            timer.send(request);
            return server.submit(SessionRequest.of(session.id(), request));
        }

        @Override
        public void onSuccess(Object result) {
            // TODO Auto-generated method stub
        }

        @Override
        public void onFailure(Throwable t) {
            if (t instanceof TimeoutException) {
                publish(SessionStateEvent.create(session(), Session.State.SESSION_EXPIRED));
                Message.ClientRequest<Records.Request> request = ProtocolRequestMessage.of(0, Records.Requests.getInstance().get(OpCode.CLOSE_SESSION));
                submit(request);
            } else {
                throw new AssertionError(t);
            }
        }
    }
}
