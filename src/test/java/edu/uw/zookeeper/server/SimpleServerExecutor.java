package edu.uw.zookeeper.server;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ProtocolResponseProcessor;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.protocol.server.SessionRequestExecutor;
import edu.uw.zookeeper.protocol.server.ToTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;

public class SimpleServerExecutor extends ServerTaskExecutor {
    
    public static SimpleServerExecutor newInstance() {
        SimpleSessionTable sessions = newSessionTable();
        ZNodeDataTrie data = ZNodeDataTrie.newInstance();
        ServerTaskExecutor tasks = newServerTaskExecutor(data);
        return new SimpleServerExecutor(sessions, data, tasks.getAnonymousExecutor(), tasks.getConnectExecutor(), tasks.getSessionExecutor());
    }

    public static SimpleSessionTable newSessionTable() {
        return new SimpleSessionTable(
                EventBusPublisher.newInstance(),
                Maps.<Long, Session>newHashMap(),
                TimeValue.create(Session.Parameters.NEVER_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    public static SessionRequestExecutor newSessionExecutor(
            Executor executor,
            ZxidGenerator zxids,
            ZNodeDataTrie dataTrie,
            final Map<Long, Publisher> listeners,
            SessionTable sessions) {
        Processor<SessionOperation.Request<?>, Message.ServerResponse<?>> processor = 
                Processors.bridge(
                        ToTxnRequestProcessor.create(
                                AssignZxidProcessor.newInstance(zxids)), 
                        ProtocolResponseProcessor.create(
                                ServerApplicationBuilder.defaultTxnProcessor(dataTrie, sessions,
                                        new Function<Long, Publisher>() {
                                            @Override
                                            public @Nullable Publisher apply(Long input) {
                                                return listeners.get(input);
                                            }
                                })));
        return SessionRequestExecutor.newInstance(executor, listeners, processor);
    }
    
    public static ServerTaskExecutor newServerTaskExecutor(
            ZNodeDataTrie dataTrie) {
        SessionTable sessions = newSessionTable();
        ZxidIncrementer zxids = ZxidIncrementer.fromZero();
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        SessionRequestExecutor sessionExecutor = newSessionExecutor(
                MoreExecutors.sameThreadExecutor(), zxids, dataTrie, listeners, sessions);
        return ServerApplicationBuilder.defaultServerExecutor(zxids, sessions, listeners, sessionExecutor);
    }
    
    protected final SessionTable sessions;
    protected final ZNodeDataTrie data;
    
    public SimpleServerExecutor(
            SessionTable sessions,
            ZNodeDataTrie data,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> sessionExecutor) {
        super(anonymousExecutor, connectExecutor, sessionExecutor);
        this.sessions = sessions;
        this.data = data;
    }
    
    public SessionTable getSessions() {
        return sessions;
    }
    
    public ZNodeDataTrie getData() {
        return data;
    }
}
