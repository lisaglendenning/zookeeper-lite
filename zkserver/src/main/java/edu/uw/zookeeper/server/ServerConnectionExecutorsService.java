package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.ForwardingService;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ByOpcodeTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ConnectListenerProcessor;
import edu.uw.zookeeper.protocol.server.ConnectTableProcessor;
import edu.uw.zookeeper.protocol.server.DisconnectTableProcessor;
import edu.uw.zookeeper.protocol.server.EphemeralProcessor;
import edu.uw.zookeeper.protocol.server.ExpiringSessionRequestExecutor;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.PingProcessor;
import edu.uw.zookeeper.protocol.server.ProtocolResponseProcessor;
import edu.uw.zookeeper.protocol.server.RequestErrorProcessor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutor;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.protocol.server.ToTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.WatcherEventProcessor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;

public class ServerConnectionExecutorsService<T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> extends ForwardingService implements Iterable<ServerConnectionExecutor<T>> {

    public static <T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> ServerConnectionExecutorsService<T> newInstance(
            ServerConnectionFactory<T> connections,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerTaskExecutor server) {
        return new ServerConnectionExecutorsService<T>(
                connections, 
                ServerConnectionExecutor.<T>factory(
                        timeOut,
                        executor,
                        server.getAnonymousExecutor(), 
                        server.getConnectExecutor(), 
                        server.getSessionExecutor()));
    }

    public static Builder builder() {
        return new Builder(null, null, null, null, null);
    }
    
    public static class Builder implements ZooKeeperApplication.RuntimeBuilder<List<Service>, Builder> {

        public static Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> defaultTxnProcessor(
                ZNodeDataTrie trie,
                final SessionTable sessions,
                Function<Long, Publisher> publishers) {
            Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> processors = Maps.newEnumMap(OpCode.class);
            processors = ZNodeDataTrie.Operators.of(trie, processors);
            processors.put(OpCode.MULTI, 
                    ZNodeDataTrie.MultiOperator.of(
                            trie, 
                            ByOpcodeTxnRequestProcessor.create(ImmutableMap.copyOf(processors))));
            processors.put(OpCode.CLOSE_SESSION, 
                    new Processors.CheckedProcessor<TxnOperation.Request<?>, IDisconnectResponse, KeeperException>() {
                        private final DisconnectTableProcessor delegate = DisconnectTableProcessor.newInstance(sessions);
                        @Override
                        public IDisconnectResponse apply(
                                TxnOperation.Request<?> request)
                                throws KeeperException {
                            return delegate.apply(request.getSessionId());
                        }
            });
            processors.put(OpCode.PING, 
                    new Processors.CheckedProcessor<TxnOperation.Request<?>, IPingResponse, KeeperException>() {
                @Override
                public IPingResponse apply(
                        TxnOperation.Request<?> request)
                        throws KeeperException {
                    return PingProcessor.getInstance().apply(request.record());
                }
            });
            return EphemeralProcessor.create(
                    WatcherEventProcessor.create(
                            RequestErrorProcessor.<TxnOperation.Request<?>>create(
                                    ByOpcodeTxnRequestProcessor.create(
                                            ImmutableMap.copyOf(processors))), 
                            publishers));
        }
        
        public static ExpiringSessionRequestExecutor defaultSessionExecutor(
                Executor executor,
                ZxidGenerator zxids,
                ZNodeDataTrie dataTrie,
                final Map<Long, Publisher> listeners,
                ExpiringSessionTable sessions) {
            Processor<SessionOperation.Request<?>, Message.ServerResponse<?>> processor = 
                    Processors.bridge(
                            ToTxnRequestProcessor.create(
                                    AssignZxidProcessor.newInstance(zxids)), 
                            ProtocolResponseProcessor.create(
                                    defaultTxnProcessor(dataTrie, sessions,
                                            new Function<Long, Publisher>() {
                                                @Override
                                                public @Nullable Publisher apply(@Nullable Long input) {
                                                    return listeners.get(input);
                                                }
                                    })));
            return ExpiringSessionRequestExecutor.newInstance(sessions, executor, listeners, processor);
        }
        
        public static ServerTaskExecutor defaultServerExecutor(
                ZxidReference zxids,
                SessionTable sessions,
                Map<Long, Publisher> listeners,
                TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> sessionExecutor) {
            TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(FourLetterRequestProcessor.newInstance());
            TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> connectExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(ConnectListenerProcessor.newInstance(
                            ConnectTableProcessor.create(sessions, zxids), listeners));
            return ServerTaskExecutor.newInstance(anonymousExecutor, connectExecutor, sessionExecutor);
        }

        protected final RuntimeModule runtime;
        protected final ServerConnectionFactoryBuilder connectionBuilder;
        protected final ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory;
        protected final ServerTaskExecutor serverTaskExecutor;
        protected final ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors;

        protected Builder(
                ServerConnectionFactoryBuilder connectionBuilder,
                ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
                ServerTaskExecutor serverTaskExecutor,
                ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
                RuntimeModule runtime) {
            this.connectionBuilder = connectionBuilder;
            this.serverConnectionFactory = serverConnectionFactory;
            this.serverTaskExecutor = serverTaskExecutor;
            this.connectionExecutors = connectionExecutors;
            this.runtime = runtime;
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
                        serverConnectionFactory, 
                        serverTaskExecutor, 
                        connectionExecutors, 
                        runtime);
            }
        }
        
        public ServerConnectionFactoryBuilder getConnectionBuilder() {
            return connectionBuilder;
        }

        public Builder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
            if (this.connectionBuilder == connectionBuilder) {
                return this;
            } else {
                return newInstance(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
            }
        }
        
        public ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getServerConnectionFactory() {
            return serverConnectionFactory;
        }

        public Builder setServerConnectionFactory(
                ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory) {
            if (this.serverConnectionFactory == serverConnectionFactory) {
                return this;
            } else {
                return newInstance(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
            }
        }

        public ServerTaskExecutor getServerTaskExecutor() {
            return serverTaskExecutor;
        }

        public Builder setServerTaskExecutor(ServerTaskExecutor serverTaskExecutor) {
            if (this.serverTaskExecutor == serverTaskExecutor) {
                return this;
            } else {
                return newInstance(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
            }
        }

        public ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getConnectionExecutors() {
            return connectionExecutors;
        }

        public Builder setConnectionExecutors(ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors) {
            if (this.connectionExecutors == connectionExecutors) {
                return this;
            } else {
                return newInstance(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
            }
        }
        
        @Override
        public List<Service> build() {
            return setDefaults().getServices();
        }

        public Builder setDefaults() {
            checkState(getRuntimeModule() != null);
        
            if (this.connectionBuilder == null) {
                return setConnectionBuilder(getDefaultServerConnectionFactoryBuilder()).setDefaults();
            }
            ServerConnectionFactoryBuilder connectionBuilder = this.connectionBuilder.setDefaults();
            if (this.connectionBuilder != connectionBuilder) {
                return setConnectionBuilder(connectionBuilder).setDefaults();
            }
            if (serverConnectionFactory == null) {
                return setServerConnectionFactory(getDefaultServerConnectionFactory()).setDefaults();
            }
            if (serverTaskExecutor == null) {
                return setServerTaskExecutor(getDefaultServerTaskExecutor()).setDefaults();
            }
            if (connectionExecutors == null) {
                return setConnectionExecutors(getDefaultConnectionExecutorsService()).setDefaults();
            }
            return this;
        }
        
        protected Builder newInstance(
                ServerConnectionFactoryBuilder connectionBuilder,
                ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
                ServerTaskExecutor serverTaskExecutor,
                ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
                RuntimeModule runtime) {
            return new Builder(connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
        }
        
        protected ServerConnectionFactoryBuilder getDefaultServerConnectionFactoryBuilder() {
            return ServerConnectionFactoryBuilder.defaults().setRuntimeModule(runtime).setDefaults();
        }

        protected ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultServerConnectionFactory() {
            return connectionBuilder.build();
        }
        
        protected ExpiringSessionTable getDefaultExpiringSessionTable() {
            SessionParametersPolicy policy = 
                    DefaultSessionParametersPolicy.create(getRuntimeModule().getConfiguration());
            ExpiringSessionTable sessions = 
                    ExpiringSessionTable.newInstance(EventBusPublisher.newInstance(), policy);
            getRuntimeModule().getServiceMonitor().add(
                    ExpiringSessionService.newInstance(
                            sessions, 
                            getRuntimeModule().getExecutors().get(ScheduledExecutorService.class),
                            getRuntimeModule().getConfiguration()));
            return sessions;
        }
        
        protected ServerTaskExecutor getDefaultServerTaskExecutor() {
            ZxidEpochIncrementer zxids = ZxidEpochIncrementer.fromZero();
            ExpiringSessionTable sessionTable = getDefaultExpiringSessionTable();
            ZNodeDataTrie dataTrie = ZNodeDataTrie.newInstance();
            ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
            ExpiringSessionRequestExecutor sessionExecutor = defaultSessionExecutor(
                    getRuntimeModule().getExecutors().get(ExecutorService.class),
                    zxids,
                    dataTrie,
                    listeners,
                    sessionTable);
            return defaultServerExecutor(
                    zxids,
                    sessionTable,
                    listeners,
                    sessionExecutor);
        }
        
        protected ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultConnectionExecutorsService() {
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> instance = ServerConnectionExecutorsService.newInstance(
                    getServerConnectionFactory(), 
                    getConnectionBuilder().getTimeOut(),
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class),
                    getServerTaskExecutor());
            return instance;
        }

        protected List<Service> getServices() {
            return Lists.<Service>newArrayList(
                    getConnectionExecutors());
        }
    }

    protected final ServerConnectionFactory<T> connections;
    protected final ParameterizedFactory<T, ServerConnectionExecutor<T>> factory;
    protected final ConcurrentMap<T, ServerConnectionExecutor<T>> handlers;
    
    public ServerConnectionExecutorsService(
            ServerConnectionFactory<T> connections,
            ParameterizedFactory<T, ServerConnectionExecutor<T>> factory) {
        this.connections = connections;
        this.factory = factory;
        this.handlers = new MapMaker().makeMap();
    }
    
    @Override
    public Iterator<ServerConnectionExecutor<T>> iterator() {
        return handlers.values().iterator();
    }

    public ServerConnectionFactory<T> connections() {
        return connections;
    }

    @Subscribe
    public void handleNewConnection(T connection) {
        new RemoveOnClose(connection, factory.get(connection));
    }

    @Override
    protected Service delegate() {
        return connections;
    }

    @Override
    protected void startUp() throws Exception {
        connections.register(this);
        
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        
        try {
            connections.unregister(this);
        } catch (IllegalArgumentException e) {}
    }

    protected class RemoveOnClose extends Pair<T, ServerConnectionExecutor<T>> {
        
        public RemoveOnClose(T connection, ServerConnectionExecutor<T> handler) {
            super(connection, handler);
            if (handlers.putIfAbsent(connection, handler) != null) {
                throw new AssertionError();
            }
            connection.register(this);
        }
    
        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    first().unregister(this);
                } catch (IllegalArgumentException e) {}
                handlers.remove(first(), second());
            }
        }
    }
}
