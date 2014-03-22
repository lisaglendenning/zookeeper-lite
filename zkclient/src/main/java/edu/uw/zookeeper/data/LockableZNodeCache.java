package edu.uw.zookeeper.data;


import static com.google.common.base.Preconditions.*;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.engio.mbassy.common.StrongConcurrentSet;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.Records;

/**
 * Use lock() when accessing cache()
 */
public class LockableZNodeCache<E extends AbstractNameTrie.SimpleNode<E> & LockableZNodeCache.CacheNode<E,?>, I extends Operation.Request, O extends Operation.ProtocolResponse<?>> 
        extends ZNodeCache<E,I,O> {

    public static <I extends Operation.Request,O extends Operation.ProtocolResponse<?>> LockableZNodeCache<SimpleCacheNode,I,O> newInstance(
            ClientExecutor<? super I,O,SessionListener> client) {
        return newInstance(client, SimpleCacheNode.root());
    }
    
    public static <E extends AbstractNameTrie.SimpleNode<E> & LockableZNodeCache.CacheNode<E,?>, I extends Operation.Request,O extends Operation.ProtocolResponse<?>> LockableZNodeCache<E,I,O> newInstance(
            ClientExecutor<? super I,O,SessionListener> client, E root) {
        return new LockableZNodeCache<E,I,O>(new ReentrantReadWriteLock(true), client, new CacheEvents(new StrongConcurrentSet<CacheListener>()), SimpleLabelTrie.forRoot(root));
    }
    
    protected final ReentrantReadWriteLock lock;
    
    protected LockableZNodeCache( 
            ReentrantReadWriteLock lock,
            ClientExecutor<? super I, O, SessionListener> client,
            CacheEvents events,
            NameTrie<E> trie) {
        super(client, events, trie);
        this.lock = checkNotNull(lock);
    }
    
    public ReentrantReadWriteLock lock() {
        return lock;
    }
    
    @Override
    protected void handleResult(Records.Request request, Operation.ProtocolResponse<?> result) {
        lock.writeLock().lock();
        try {
            super.handleResult(request, result);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
