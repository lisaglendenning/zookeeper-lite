package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ServerResponse;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;

public class SimpleServer extends ExecutedActor<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> implements TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> {
    
    public static abstract class Builder<C extends Builder<C>> implements ZooKeeperApplication.RuntimeBuilder<SimpleServer, C> {

        protected final RuntimeModule runtime;
        protected final ZxidGenerator zxids;
        protected final ZNodeDataTrie data;
        protected final SessionManager sessions;
        protected final Function<Long, ? extends PubSubSupport<? super Message.ServerResponse<?>>> publishers;
        
        protected Builder(
                ZxidGenerator zxids,
                ZNodeDataTrie data,
                SessionManager sessions,
                Function<Long, ? extends PubSubSupport<? super Message.ServerResponse<?>>> publishers,
                RuntimeModule runtime) {
            this.zxids = zxids;
            this.data = data;
            this.sessions = sessions;
            this.runtime = runtime;
            this.publishers = publishers;
        }
        
        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @Override
        public C setRuntimeModule(RuntimeModule runtime) {
            return newInstance(zxids, data, sessions, publishers, runtime);
        }
        
        public ZxidGenerator getZxids() {
            return zxids;
        }
        
        public C setZxids(ZxidGenerator zxids) {
            return newInstance(zxids, data, sessions, publishers, runtime);
        }
        
        public ZxidGenerator getDefaultZxids() {
            return ZxidEpochIncrementer.fromZero();
        }
        
        public ZNodeDataTrie getData() {
            return data;
        }

        public C setData(ZNodeDataTrie data) {
            return newInstance(zxids, data, sessions, publishers, runtime);
        }
        
        public ZNodeDataTrie getDefaultData() {
            return ZNodeDataTrie.newInstance();
        }

        public SessionManager getSessions() {
            return sessions;
        }

        public C setSessions(SessionManager sessions) {
            return newInstance(zxids, data, sessions, publishers, runtime);
        }

        public Function<Long, ? extends PubSubSupport<? super Message.ServerResponse<?>>> getPublishers() {
            return publishers;
        }

        public C setPublishers(Function<Long, ? extends PubSubSupport<? super Message.ServerResponse<?>>> publishers) {
            return newInstance(zxids, data, sessions, publishers, runtime);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public C setDefaults() {
            checkState(getRuntimeModule() != null);
            if (getZxids() == null) {
                return setZxids(getDefaultZxids()).setDefaults();
            }
            if (getData() == null) {
                return setData(getDefaultData()).setDefaults();
            }
            if (getSessions() == null) {
                return setSessions(getDefaultSessions()).setDefaults();
            }
            if (getPublishers() == null) {
                return setPublishers(getDefaultPublishers()).setDefaults();
            }
            return (C) this;
        }

        @Override
        public SimpleServer build() {
            return setDefaults().doBuild();
        }
        
        protected abstract C newInstance(
                ZxidGenerator zxids,
                ZNodeDataTrie data,
                SessionManager sessions,
                Function<Long, ? extends PubSubSupport<? super Message.ServerResponse<?>>> publishers,
                RuntimeModule runtime);

        protected SimpleServer doBuild() {
            return new SimpleServer(
                    getDefaultProcessor(), 
                    getRuntimeModule().getExecutors().get(ExecutorService.class));
        }

        protected abstract Function<Long, ? extends PubSubSupport<? super ServerResponse<?>>> getDefaultPublishers();

        protected abstract SessionManager getDefaultSessions();

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
            processors = ZNodeDataTrie.Operators.of(getData(), processors);
            processors.put(OpCode.MULTI, 
                    ZNodeDataTrie.MultiOperator.of(
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
                    WatcherEventProcessor.create(
                            RequestErrorProcessor.<TxnOperation.Request<?>>create(
                                    ByOpcodeTxnRequestProcessor.create(
                                            ImmutableMap.copyOf(processors))), 
                            getPublishers()));
        }
    }
    
    protected final Logger logger;
    protected final Executor executor;
    protected final Queue<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> mailbox;
    protected final Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor;
    
    protected SimpleServer(
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor,
            Executor executor) {
        this.logger = LogManager.getLogger(getClass());
        this.executor = executor;
        this.mailbox = Queues.newConcurrentLinkedQueue();
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
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                try {
                    Message.ServerResponse<?> response = processor.apply(input.task());
                    input.set(response);
                } catch (Exception e) {
                    input.setException(e);
                }
            } else {
                input.cancel(true);
            }
        }
        return (state() != State.TERMINATED);
    }
    
    @Override
    protected void doStop() {
        Future<?> task;
        while ((task = mailbox.poll()) != null) {
            task.cancel(true);
        }
    }

    @Override
    protected Queue<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}