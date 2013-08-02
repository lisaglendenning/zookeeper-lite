package edu.uw.zookeeper.data;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodeDataTrie.Operators;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ByOpcodeTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.RequestErrorProcessor;
import edu.uw.zookeeper.protocol.server.ToTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;

public class ZNodeDataTrieExecutor implements Publisher, ClientExecutor<SessionOperation.Request<Records.Request>, SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>,
        Processors.UncheckedProcessor<SessionOperation.Request<Records.Request>, Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>> {

    public static ZNodeDataTrieExecutor create() {
        return new ZNodeDataTrieExecutor(
                ZNodeDataTrie.newInstance(),
                ZxidIncrementer.fromZero(),
                EventBusPublisher.newInstance());
    }
    
    protected final Publisher publisher;
    protected final ZNodeDataTrie trie;
    protected final RequestErrorProcessor<TxnOperation.Request<Records.Request>> operator;
    protected final ToTxnRequestProcessor txnProcessor;
    
    public ZNodeDataTrieExecutor(
            ZNodeDataTrie trie,
            Generator<Long> zxids,
            Publisher publisher) {
        this.trie = trie;
        this.publisher = publisher;
        this.txnProcessor = ToTxnRequestProcessor.create(
                AssignZxidProcessor.newInstance(zxids));
        this.operator = RequestErrorProcessor.create(
                ByOpcodeTxnRequestProcessor.create(
                        ImmutableMap.copyOf(Operators.of(trie))));
    }
    
    @Override
    public ListenableFuture<Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>> submit(
            SessionOperation.Request<Records.Request> request) {
        return submit(request, SettableFuturePromise.<Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>>create());
    }

    @Override
    public synchronized ListenableFuture<Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>> submit(
            SessionOperation.Request<Records.Request> request,
            Promise<Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>> promise) {
        Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> result = apply(request);
        promise.set(result);
        return promise;
    }
    
    @Override
    public synchronized Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> apply(SessionOperation.Request<Records.Request> input) {
        TxnOperation.Request<Records.Request> request = txnProcessor.apply(input);
        Message.ServerResponse<Records.Response> response = ProtocolResponseMessage.of(request.getXid(), request.getZxid(), operator.apply(request));
        post(response);
        return Pair.create(input, response);
    }

    @Override
    public void register(Object handler) {
        publisher.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        publisher.unregister(handler);
    }
    
    @Override
    public void post(Object event) {
        publisher.post(event);
    }
}