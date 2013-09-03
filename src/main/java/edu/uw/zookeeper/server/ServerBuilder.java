package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkState;

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
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.*;
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
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.*;

public class ServerBuilder implements ZooKeeperApplication.RuntimeBuilder<List<? extends Service>> {

    public static ServerBuilder defaults() {
        return new ServerBuilder();
    }
    
    @Configurable(arg="clientAddress", key="ClientAddress", value=":2181", help="Address:Port")
    public static class ConfigurableServerAddressView implements Function<Configuration, ServerInetAddressView> {

        public static ServerInetAddressView get(Configuration configuration) {
            return new ConfigurableServerAddressView().apply(configuration);
        }
        
        @Override
        public ServerInetAddressView apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return ServerInetAddressView.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }
    }

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

    protected final ServerConnectionFactoryBuilder connectionBuilder;
    protected final ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory;
    protected final ServerTaskExecutor serverTaskExecutor;
    
    protected ServerBuilder() {
        this(ServerConnectionFactoryBuilder.defaults(), null, null);
    }

    protected ServerBuilder(
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor) {
        this.connectionBuilder = connectionBuilder;
        this.serverConnectionFactory = serverConnectionFactory;
        this.serverTaskExecutor = serverTaskExecutor;
    }

    @Override
    public RuntimeModule getRuntimeModule() {
        return connectionBuilder.getRuntimeModule();
    }

    @Override
    public ServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return new ServerBuilder(connectionBuilder.setRuntimeModule(runtime), serverConnectionFactory, serverTaskExecutor);
    }
    
    public ServerConnectionFactoryBuilder getConnectionBuilder() {
        return connectionBuilder;
    }

    public ServerBuilder setConnectionBuilder(ServerConnectionFactoryBuilder connectionBuilder) {
        return new ServerBuilder(connectionBuilder, serverConnectionFactory, serverTaskExecutor);
    }
    
    public ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getServerConnectionFactory() {
        return serverConnectionFactory;
    }

    public ServerBuilder setServerConnectionFactory(
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory) {
        return new ServerBuilder(connectionBuilder, serverConnectionFactory, serverTaskExecutor);
    }

    public ServerTaskExecutor getServerTaskExecutor() {
        return serverTaskExecutor;
    }

    public ServerBuilder setServerTaskExecutor(ServerTaskExecutor serverTaskExecutor) {
        return new ServerBuilder(connectionBuilder, serverConnectionFactory, serverTaskExecutor);
    }

    @Override
    public List<? extends Service> build() {
        return setDefaults().getServices();
    }

    public ServerBuilder setDefaults() {
        checkState(getRuntimeModule() != null);
    
        if (serverConnectionFactory == null) {
            return setServerConnectionFactory(connectionBuilder.build());
        } else if (serverTaskExecutor == null) {
            return setServerTaskExecutor(getDefaultServerTaskExecutor());
        } else {
            return this;
        }
    }

    protected ExpiringSessionTable getDefaultExpiringSessionTable() {
        SessionParametersPolicy policy = 
                DefaultSessionParametersPolicy.create(getRuntimeModule().configuration());
        ExpiringSessionTable sessions = 
                ExpiringSessionTable.newInstance(EventBusPublisher.newInstance(), policy);
        getRuntimeModule().serviceMonitor().add(
                ExpiringSessionService.newInstance(
                        sessions, 
                        getRuntimeModule().executors().get(ScheduledExecutorService.class),
                        getRuntimeModule().configuration()));
        return sessions;
    }
    
    protected ServerTaskExecutor getDefaultServerTaskExecutor() {
        ZxidEpochIncrementer zxids = ZxidEpochIncrementer.fromZero();
        ExpiringSessionTable sessionTable = getDefaultExpiringSessionTable();
        ZNodeDataTrie dataTrie = ZNodeDataTrie.newInstance();
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ExpiringSessionRequestExecutor sessionExecutor = defaultSessionExecutor(
                getRuntimeModule().executors().get(ExecutorService.class),
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
                serverConnectionFactory, 
                connectionBuilder.getTimeOut(),
                getRuntimeModule().executors().get(ScheduledExecutorService.class),
                serverTaskExecutor);
        return instance;
    }

    protected List<? extends Service> getServices() {
        return Lists.<Service>newArrayList(
                getDefaultConnectionExecutorsService());
    }
}
