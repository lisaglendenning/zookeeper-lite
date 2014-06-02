package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeNode;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.ByOpcodeTxnRequestProcessor;
import edu.uw.zookeeper.server.RequestErrorProcessor;
import edu.uw.zookeeper.server.ToTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;

// FIXME: notifications
public class ZNodeTrieExecutor implements ClientExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>, SessionListener>,
        Processors.UncheckedProcessor<SessionOperation.Request<?>, Message.ServerResponse<?>> {

    public static ZNodeTrieExecutor defaults() {
        return newInstance(
                SimpleLabelTrie.forRoot(ZNodeNode.root()),
                ZxidIncrementer.fromZero(),
                new StrongConcurrentSet<SessionListener>());
    }

    public static ZNodeTrieExecutor newInstance(
            NameTrie<ZNodeNode> trie,
            ZxidGenerator zxids,
            IConcurrentSet<SessionListener> listeners) {
        return new ZNodeTrieExecutor(trie, zxids, listeners);
    }
    
    protected final IConcurrentSet<SessionListener> listeners;
    protected final NameTrie<ZNodeNode> trie;
    protected final RequestErrorProcessor<TxnOperation.Request<?>> operator;
    protected final ToTxnRequestProcessor txnProcessor;
    
    public ZNodeTrieExecutor(
            NameTrie<ZNodeNode> trie,
            ZxidGenerator zxids,
            IConcurrentSet<SessionListener> listeners) {
        this.trie = checkNotNull(trie);
        this.listeners = checkNotNull(listeners);
        this.txnProcessor = ToTxnRequestProcessor.create(
                AssignZxidProcessor.newInstance(zxids));
        this.operator = RequestErrorProcessor.create(
                ByOpcodeTxnRequestProcessor.create(
                        ImmutableMap.copyOf(ZNodeNode.Operators.of(trie))));
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request) {
        return submit(request, SettableFuturePromise.<Message.ServerResponse<?>>create());
    }

    @Override
    public synchronized ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request,
            Promise<Message.ServerResponse<?>> promise) {
        Message.ServerResponse<?> result = apply(request);
        promise.set(result);
        return promise;
    }
    
    @Override
    public synchronized Message.ServerResponse<?> apply(SessionOperation.Request<?> input) {
        TxnOperation.Request<?> request = txnProcessor.apply(input);
        Message.ServerResponse<Records.Response> response = ProtocolResponseMessage.of(request.xid(), request.zxid(), operator.apply(request));
        return response;
    }

    @Override
    public void subscribe(SessionListener listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return listeners.remove(listener);
    }
}