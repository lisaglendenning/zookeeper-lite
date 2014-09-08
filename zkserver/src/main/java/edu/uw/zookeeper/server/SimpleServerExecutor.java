package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;
import com.google.common.hash.Hashing;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.ZNodeNode;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.SessionExecutor;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;

public class SimpleServerExecutor<T extends SessionExecutor> implements ServerExecutor<T> {

    public static Builder builder(ServerConnectionFactoryBuilder connections) {
        return Builder.defaults(connections);
    }

    public static class Builder implements ZooKeeperApplication.RuntimeBuilder<SimpleServerExecutor<?>, Builder> {

        public static Builder defaults(ServerConnectionFactoryBuilder connections) {
            return new Builder(ServerBuilder.defaults(connections));
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
            @SuppressWarnings("unchecked")
            SimpleSessionManager<SimpleSessionExecutor> sessions = (SimpleSessionManager<SimpleSessionExecutor>) getServer().getSessions();
            SimpleConnectExecutor<SimpleSessionExecutor> connect = SimpleConnectExecutor.defaults(sessions.executors(), sessions, getServer().getZxids());
            return new SimpleServerExecutor<SimpleSessionExecutor>(
                    getServer().getSessionExecutors(),
                    connect,
                    getDefaultAnonymousExecutor());
        }
        
        protected TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> getDefaultAnonymousExecutor() {
            return ProcessorTaskExecutor.of(FourLetterRequestProcessor.create(getServer()));
        }
    }
    
    public static class ServerBuilder extends SimpleServer.Builder<ServerBuilder> {

        public static ServerBuilder defaults(ServerConnectionFactoryBuilder connections) {
            return new ServerBuilder(connections, new SimpleServerSupplier(null), null, null, null, null, null, null, null, null, null);
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
        
        protected final ServerConnectionFactoryBuilder connections;
        protected final ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors;
        protected final SimpleServerSupplier server;
        
        protected ServerBuilder(
                ServerConnectionFactoryBuilder connections,
                SimpleServerSupplier server,
                ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors,
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            super(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
            this.connections = checkNotNull(connections);
            this.server = checkNotNull(server);
            this.sessionExecutors = sessionExecutors;
        }

        public ConcurrentMap<Long, SimpleSessionExecutor> getSessionExecutors() {
            return sessionExecutors;
        }

        public ServerBuilder setSessionExecutors(ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors) {
            return newInstance(connections, server, sessionExecutors, zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
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
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            return newInstance(connections, server, sessionExecutors, zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }

        protected ServerBuilder newInstance(
                ServerConnectionFactoryBuilder connections,
                SimpleServerSupplier server,
                ConcurrentMap<Long, SimpleSessionExecutor> sessionExecutors,
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            return new ServerBuilder(connections, server, sessionExecutors, zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
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
        protected SimpleSessionManager<SimpleSessionExecutor> getDefaultSessions() {
            return SimpleSessionManager.fromConfiguration(
                    getDefaultSessionId(),
                    getSessionExecutors(), 
                    getDefaultSessionFactory(), 
                    getRuntimeModule().getConfiguration());
        }
        
        protected ConcurrentMap<Long, SimpleSessionExecutor> getDefaultSessionExecutors() {
            return new MapMaker().makeMap();
        }

        protected ParameterizedFactory<Session, SimpleSessionExecutor> getDefaultSessionFactory() {
            return SimpleSessionExecutor.factory(
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class), 
                    server);
        }
        
        protected short getDefaultSessionId() {
            ServerInetAddressView address = connections.setRuntimeModule(getRuntimeModule()).setDefaults().getAddress();
            return (short) Hashing.murmur3_32().hashInt(address.get().hashCode()).asInt();
        }
    }

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
}
