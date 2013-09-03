package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkState;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.*;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;
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

public abstract class ServerApplicationBuilder<T extends Application> extends ZooKeeperApplication.ApplicationBuilder<T> {

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

    protected TimeValue timeOut;
    protected NetServerModule serverModule;
    protected ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory;
    protected ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory;
    protected ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory;
    protected ServerTaskExecutor serverTaskExecutor;
    
    protected ServerApplicationBuilder() {
    }

    public TimeValue getTimeOut() {
        return timeOut;
    }

    public ServerApplicationBuilder<T> setTimeOut(TimeValue timeOut) {
        this.timeOut = timeOut;
        return this;
    }

    public NetServerModule getServerModule() {
        return serverModule;
    }

    public ServerApplicationBuilder<T> setServerModule(NetServerModule serverModule) {
        this.serverModule = serverModule;
        return this;
    }

    public ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> getCodecFactory() {
        return codecFactory;
    }

    public ServerApplicationBuilder<T> setCodecFactory(
            ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory) {
        this.codecFactory = codecFactory;
        return this;
    }

    public ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getConnectionFactory() {
        return connectionFactory;
    }

    public ServerApplicationBuilder<T> setConnectionFactory(
            ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getServerConnectionFactory() {
        return serverConnectionFactory;
    }

    public ServerApplicationBuilder<T> setServerConnectionFactory(
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory) {
        this.serverConnectionFactory = serverConnectionFactory;
        return this;
    }

    public ServerTaskExecutor getServerTaskExecutor() {
        return serverTaskExecutor;
    }

    public ServerApplicationBuilder<T> setServerTaskExecutor(ServerTaskExecutor serverTaskExecutor) {
        this.serverTaskExecutor = serverTaskExecutor;
        return this;
    }

    protected TimeValue getDefaultTimeOut() {
        return ZooKeeperApplication.ConfigurableTimeout.get(runtime.configuration());
    }
    
    protected NetServerModule getDefaultNetServerModule() {
        return NettyServerModule.newInstance(runtime);
    }

    protected ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> getDefaultCodecFactory() {
        return ServerProtocolCodec.factory();
    }
    
    protected ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultConnectionFactory() {
        return ProtocolCodecConnection.factory();
    }
    
    protected ExpiringSessionTable getDefaultExpiringSessionTable() {
        SessionParametersPolicy policy = 
                DefaultSessionParametersPolicy.create(runtime.configuration());
        ExpiringSessionTable sessions = 
                ExpiringSessionTable.newInstance(EventBusPublisher.newInstance(), policy);
        runtime.serviceMonitor().add(
                ExpiringSessionService.newInstance(
                        sessions, 
                        runtime.executors().get(ScheduledExecutorService.class),
                        runtime.configuration()));
        return sessions;
    }
    
    protected ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultServerConnectionFactory() {
        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>> serverConnectionFactory = 
                serverModule.getServerConnectionFactory(
                        codecFactory,
                        connectionFactory);
        ServerInetAddressView address = getDefaultAddress();
        ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                serverConnectionFactory.get(address.get());
        return serverConnections;
    }
    
    protected ServerInetAddressView getDefaultAddress() {
        return ConfigurableServerAddressView.get(runtime.configuration());
    }
    
    protected ServerTaskExecutor getDefaultServerTaskExecutor() {
        ZxidEpochIncrementer zxids = ZxidEpochIncrementer.fromZero();
        ExpiringSessionTable sessionTable = getDefaultExpiringSessionTable();
        ZNodeDataTrie dataTrie = ZNodeDataTrie.newInstance();
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ExpiringSessionRequestExecutor sessionExecutor = defaultSessionExecutor(
                runtime.executors().get(ExecutorService.class),
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
    
    protected ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getDefaultServerConnectionExecutorsService() {
        ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> instance = ServerConnectionExecutorsService.newInstance(
                serverConnectionFactory, 
                timeOut,
                runtime.executors().get(ScheduledExecutorService.class),
                serverTaskExecutor);
        return instance;
    }

    @Override
    public T build() {
        getDefaults();
        return getApplication();
    }
    
    protected void getDefaults() {
        checkState(runtime != null);
        
        if (serverModule == null) {
            serverModule = getDefaultNetServerModule();
        }

        if (timeOut == null) {
            timeOut = getDefaultTimeOut();
        }

        if (codecFactory == null) {
            codecFactory = getDefaultCodecFactory();
        }
        
        if (connectionFactory == null) {
            connectionFactory = getDefaultConnectionFactory();
        }
        
        if (serverConnectionFactory == null) {
            serverConnectionFactory = getDefaultServerConnectionFactory();
        }
        
        if (serverTaskExecutor == null) {
            serverTaskExecutor = getDefaultServerTaskExecutor();
        }
    }

    protected abstract T getApplication();
}
