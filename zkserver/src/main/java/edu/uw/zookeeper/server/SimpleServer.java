package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Actors.ExecutedQueuedActor;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.ZNodeNode;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;

public class SimpleServer extends ExecutedQueuedActor<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> implements TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> {
    
    public static abstract class Builder<C extends Builder<C>> implements ZooKeeperApplication.RuntimeBuilder<SimpleServer, C> {

        protected final RuntimeModule runtime;
        protected final ZxidGenerator zxids;
        protected final NameTrie<ZNodeNode> data;
        protected final SessionManager sessions;
        protected final ReentrantReadWriteLock lock;
        protected final Watches dataWatches;
        protected final Watches childWatches;
        protected final Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners;
        
        protected Builder(
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime) {
            this.zxids = zxids;
            this.data = data;
            this.sessions = sessions;
            this.dataWatches = dataWatches;
            this.childWatches = childWatches;
            this.runtime = runtime;
            this.listeners = listeners;
            this.lock = lock;
        }
        
        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @Override
        public C setRuntimeModule(RuntimeModule runtime) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        public ReentrantReadWriteLock getLock() {
            return lock;
        }
        
        public C setLock(ReentrantReadWriteLock lock) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        public Watches getDataWatches() {
            return dataWatches;
        }
        
        public C setDataWatches(Watches dataWatches) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        public Watches getChildWatches() {
            return childWatches;
        }
        
        public C setChildWatches(Watches childWatches) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        public ZxidGenerator getZxids() {
            return zxids;
        }
        
        public C setZxids(ZxidGenerator zxids) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        public ZxidGenerator getDefaultZxids() {
            return ZxidEpochIncrementer.fromZero();
        }
        
        public NameTrie<ZNodeNode> getData() {
            return data;
        }

        public C setData(NameTrie<ZNodeNode> data) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        public NameTrie<ZNodeNode> getDefaultData() {
            return SimpleLabelTrie.forRoot(ZNodeNode.root());
        }

        public SessionManager getSessions() {
            return sessions;
        }

        public C setSessions(SessionManager sessions) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }

        public Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> getListeners() {
            return listeners;
        }

        public C setListeners(Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners) {
            return newInstance(zxids, data, sessions, lock, dataWatches, childWatches, listeners, runtime);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public C setDefaults() {
            checkState(getRuntimeModule() != null);
            if (getLock() == null) {
                return setLock(getDefaultLock()).setDefaults();
            }
            if (getZxids() == null) {
                return setZxids(getDefaultZxids()).setDefaults();
            }
            if (getData() == null) {
                return setData(getDefaultData()).setDefaults();
            }
            if (getSessions() == null) {
                return setSessions(getDefaultSessions()).setDefaults();
            }
            if (getListeners() == null) {
                return setListeners(getDefaultListeners()).setDefaults();
            }
            if (getDataWatches() == null) {
                return setDataWatches(getDefaultWatches()).setDefaults();
            }
            if (getChildWatches() == null) {
                return setChildWatches(getDefaultWatches()).setDefaults();
            }
            return (C) this;
        }

        @Override
        public SimpleServer build() {
            return setDefaults().doBuild();
        }
        
        protected abstract C newInstance(
                ZxidGenerator zxids,
                NameTrie<ZNodeNode> data,
                SessionManager sessions,
                ReentrantReadWriteLock lock,
                Watches dataWatches,
                Watches childWatches,
                Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> listeners,
                RuntimeModule runtime);

        protected SimpleServer doBuild() {
            return SimpleServer.newInstance(
                    getLock(),
                    getDefaultProcessor(), 
                    getRuntimeModule().getExecutors().get(ExecutorService.class));
        }

        protected abstract Function<Long, ? extends NotificationListener<Operation.ProtocolResponse<IWatcherEvent>>> getDefaultListeners();

        protected abstract SessionManager getDefaultSessions();
        
        protected ReentrantReadWriteLock getDefaultLock() {
            return new ReentrantReadWriteLock();
        }
        
        protected Watches getDefaultWatches() {
            return Watches.create(getListeners());
        }

        protected Processor<SessionOperation.Request<?>, Message.ServerResponse<?>> getDefaultProcessor() {
            Processor<SessionOperation.Request<?>, Message.ServerResponse<?>> processor = 
                    Processors.bridge(
                            ToTxnRequestProcessor.create(
                                    AssignZxidProcessor.newInstance(getZxids())), 
                            ProtocolResponseProcessor.create(
                                    getDefaultTxnProcessor()));
            return processor;
        }
        
        protected Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> getDefaultTxnProcessor() {
            Map<OpCode, Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> processors = Maps.newEnumMap(OpCode.class);
            processors = ZNodeNode.Operators.of(getData(), processors);
            processors.put(OpCode.MULTI, 
                    ZNodeNode.MultiOperator.of(
                            getData(), 
                            ByOpcodeTxnRequestProcessor.create(ImmutableMap.copyOf(processors))));
            processors.put(OpCode.CLOSE_SESSION, 
                    new Processors.CheckedProcessor<TxnOperation.Request<?>, IDisconnectResponse, KeeperException>() {
                        @Override
                        public IDisconnectResponse apply(
                                TxnOperation.Request<?> request)
                                throws KeeperException {
                            if (getSessions().remove(request.getSessionId()) == null) {
                                throw new KeeperException.SessionMovedException();
                            }
                            return Records.newInstance(IDisconnectResponse.class);
                        }
            });
            processors.put(OpCode.PING, 
                    new Processors.CheckedProcessor<TxnOperation.Request<?>, IPingResponse, KeeperException>() {
                @Override
                public IPingResponse apply(
                        TxnOperation.Request<?> request)
                        throws KeeperException {
                    return Records.newInstance(IPingResponse.class);
                }
            });
            return EphemeralProcessor.create(
                    RequestErrorProcessor.<TxnOperation.Request<?>>create(
                            WatcherEventProcessor.create(
                                            ByOpcodeTxnRequestProcessor.create(
                                                    ImmutableMap.copyOf(processors)),
                                            getDataWatches(), getChildWatches())));
        }
    }
    
    public static SimpleServer newInstance(
            ReentrantReadWriteLock lock,
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor,
            Executor executor) {
        return new SimpleServer(
                lock,
                processor,
                executor,
                Queues.<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>>newConcurrentLinkedQueue(),
                LogManager.getLogger(SimpleServer.class));
    }

    protected final ReentrantReadWriteLock lock;
    protected final Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor;
    
    protected SimpleServer(
            ReentrantReadWriteLock lock,
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor,
            Executor executor,
            Queue<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> mailbox,
            Logger logger) {
        super(executor, mailbox, logger);
        this.lock = lock;
        this.processor = processor;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(SessionOperation.Request<?> request) {
        PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>> task = PromiseTask.<SessionOperation.Request<?>, Message.ServerResponse<?>>of(request);
        if (! send(task)) { 
            task.cancel(true);
        }
        return task;
    }

    @Override
    protected boolean apply(PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>> input) {
        lock.writeLock().lock();
        try {
            if (! input.isDone()) {
                try {
                    Message.ServerResponse<?> response = processor.apply(input.task());
                    input.set(response);
                } catch (Exception e) {
                    input.setException(e);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        return true;
    }
    
    @Override
    protected void doStop() {
        Future<?> task;
        while ((task = mailbox.poll()) != null) {
            task.cancel(true);
        }
    }
}
