package edu.uw.zookeeper.server;

import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Ping;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.IDisconnectRequest;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.*;
import edu.uw.zookeeper.util.*;

public enum ServerApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static ServerApplicationModule getInstance() {
        return INSTANCE;
    }

    public static class ConfigurableServerAddressViewFactory implements DefaultsFactory<Configuration, ServerInetAddressView> {

        public static ConfigurableServerAddressViewFactory newInstance() {
            return newInstance(DEFAULT_CONFIG_PATH, DEFAULT_CONFIG_KEY, DEFAULT_ARG, DEFAULT_ADDRESS, DEFAULT_PORT);
        }

        public static ConfigurableServerAddressViewFactory newInstance(
                String arg, String configKey, String configPath, String defaultAddress, int defaultPort) {
            return new ConfigurableServerAddressViewFactory(configPath,configKey, arg, defaultAddress, defaultPort);
        }
        
        public static final String DEFAULT_ARG = "clientAddress";
        public static final String DEFAULT_CONFIG_KEY = "ClientAddress";
        public static final String DEFAULT_CONFIG_PATH = "";
        public static final String DEFAULT_ADDRESS = "";
        public static final int DEFAULT_PORT = 2181;

        private final String configPath;
        private final String configKey;
        private final String arg;
        private final String defaultAddress;
        private final int defaultPort;
        
        public ConfigurableServerAddressViewFactory(
                String configPath, String configKey, String arg, String defaultAddress, int defaultPort) {
            this.arg = arg;
            this.configKey = configKey;
            this.configPath = configPath;
            this.defaultAddress = defaultAddress;
            this.defaultPort = defaultPort;
        }
        
        @Override
        public ServerInetAddressView get() {
            return ServerInetAddressView.of(
                    defaultAddress, defaultPort);
        }

        @Override
        public ServerInetAddressView get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(arg)) {
                arguments.add(arguments.newOption(arg, "Address"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(arg, configKey);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(configPath, args);
            if (config.hasPath(configKey)) {
            String input = config.getString(configKey);
                return ServerInetAddressView.fromString(input);
            } else {
                return get();
            }
        }
    }

    public static ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>> codecFactory() {
        return new ParameterizedFactory<Publisher, Pair<Class<Message.Server>, ServerProtocolCodec>>() {
            @Override
            public Pair<Class<Message.Server>, ServerProtocolCodec> get(
                    Publisher value) {
                return Pair.create(Message.Server.class, ServerProtocolCodec.newInstance(value));
            }
        };
    }

    public static ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionFactory() {
        return new ParameterizedFactory<Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>() {
            @Override
            public ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>> get(
                    Pair<Pair<Class<Message.Server>, ServerProtocolCodec>, Connection<Message.Server>> value) {
                return ProtocolCodecConnection.newInstance(value.first().second(), value.second());
            }
        };
    }
    
    public static Processors.UncheckedProcessor<TxnOperation.Request<Records.Request>, Records.Response> defaultTxnProcessor(
            ZNodeDataTrie trie,
            final SessionTable sessions,
            Function<Long, Publisher> publishers) {
        Map<OpCode, TxnRequestProcessor<?,?>> processors = Maps.newEnumMap(OpCode.class);
        processors = ZNodeDataTrie.Operators.of(trie, processors);
        processors.put(OpCode.MULTI, 
                ZNodeDataTrie.MultiOperator.of(
                        trie, 
                        ByOpcodeTxnRequestProcessor.create(ImmutableMap.copyOf(processors))));
        processors.put(OpCode.CLOSE_SESSION, 
                new TxnRequestProcessor<IDisconnectRequest, IDisconnectResponse>() {
                    private final DisconnectTableProcessor delegate = DisconnectTableProcessor.newInstance(sessions);
                    @Override
                    public IDisconnectResponse apply(
                            TxnOperation.Request<IDisconnectRequest> request)
                            throws KeeperException {
                        return delegate.apply(request);
                    }
        });
        processors.put(OpCode.PING, 
                new TxnRequestProcessor<Ping.Request, Ping.Response>() {
            @Override
            public Ping.Response apply(
                    TxnOperation.Request<Ping.Request> request)
                    throws KeeperException {
                return PingProcessor.getInstance().apply(request.getRecord());
            }
        });
        return EphemeralProcessor.create(
                WatcherEventProcessor.create(
                        RequestErrorProcessor.<TxnOperation.Request<Records.Request>>create(
                                ByOpcodeTxnRequestProcessor.create(
                                        ImmutableMap.copyOf(processors))), 
                        publishers));
    }
    
    public static ExpiringSessionRequestExecutor defaultSessionExecutor(
            Executor executor,
            Generator<Long> zxids,
            ZNodeDataTrie dataTrie,
            final Map<Long, Publisher> listeners,
            ExpiringSessionTable sessions) {
        Processor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> processor = 
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
            Generator<Long> zxids,
            SessionTable sessions,
            Map<Long, Publisher> listeners,
            TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> sessionExecutor) {
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(FourLetterRequestProcessor.getInstance());
        TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> connectExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(ConnectListenerProcessor.newInstance(
                        ConnectTableProcessor.create(sessions, zxids), listeners));
        return ServerTaskExecutor.newInstance(anonymousExecutor, connectExecutor, sessionExecutor);
    }
    
    @Override
    public Application get(RuntimeModule runtime) {
        ServiceMonitor monitor = runtime.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        NettyServerModule nettyServer = NettyServerModule.newInstance(runtime);

        SessionParametersPolicy policy = 
                DefaultSessionParametersPolicy.create(runtime.configuration());
        ExpiringSessionTable sessions = 
                ExpiringSessionTable.newInstance(runtime.publisherFactory().get(), policy);
        monitorsFactory.apply(ExpiringSessionService.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration()));

        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>> serverConnectionFactory = 
                nettyServer.get(
                        codecFactory(),
                        connectionFactory());
        ServerInetAddressView address = ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                monitorsFactory.apply(serverConnectionFactory.get(address.get()));
        
        ZxidEpochIncrementer zxids = ZxidEpochIncrementer.fromZero();
        ZNodeDataTrie dataTrie = ZNodeDataTrie.newInstance();
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ExpiringSessionRequestExecutor sessionExecutor = defaultSessionExecutor(
                runtime.executors().asListeningExecutorServiceFactory().get(),
                zxids,
                dataTrie,
                listeners,
                sessions);
        ServerTaskExecutor serverExecutor = defaultServerExecutor(
                zxids,
                sessions,
                listeners,
                sessionExecutor);
        monitorsFactory.apply(ServerConnectionExecutorsService.newInstance(serverConnections, serverExecutor));
        
        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }

}
