package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.RandomSingleClientTest;
import edu.uw.zookeeper.SimpleServerAndClient;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class TreeWalkerTest {

    protected final Logger logger = LogManager.getLogger(this);
    
    @Test(timeout=30000)
    public void testRandom() throws Exception {
        SimpleServerAndClient client = SimpleServerAndClient.defaults().setDefaults();
        ServiceMonitor monitor = client.getRuntimeModule().getServiceMonitor();
        for (Service service: client.build()) {
            monitor.add(service);
        }
        monitor.startAsync().awaitRunning();
        
        LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, Message.ServerResponse<?>> cache = 
                RandomSingleClientTest.randomCache(
                        100, 
                        TimeValue.milliseconds(5000), 
                        client.getClientBuilder().getConnectionClientExecutor(), 
                        logger);
        
        SimpleLabelTrie<SimpleTrieBuilder.Node> walk = 
                LoggingFutureListener.listen(
                        logger,
                        TreeWalker.forResult(SimpleTrieBuilder.create())
                            .setClient(client.getClientBuilder().getConnectionClientExecutor())
                            .setRequests(TreeWalker.toRequests(TreeWalker.parameters().setSync(true)))
                            .build()).get().get();
        
        Iterator<? extends NameTrie.Node<?>> cached = SortedTraversal.forRoot(cache.cache());
        Iterator<? extends NameTrie.Node<?>> walked = SortedTraversal.forRoot(walk);
        while (cached.hasNext()) {
            assertEquals(cached.next().path(), walked.next().path());
        }
        assertFalse(walked.hasNext());
        
        monitor.stopAsync().awaitTerminated();
    }
    
    public static class SortedTraversal<E extends NameTrie.Node<E>> extends AbstractNameTrie.PreOrderTraversal<E> {

        public static <E extends NameTrie.Node<E>> SortedTraversal<E> forRoot(NameTrie<E> trie) {
            return forNode(trie.root());
        }
        
        public static <E extends NameTrie.Node<E>> SortedTraversal<E> forNode(E node) {
            return new SortedTraversal<E>(node);
        }
        
        protected SortedTraversal(E root) {
            super(root);
        }

        @Override
        protected Iterable<E> childrenOf(E node) {
            return ImmutableSortedMap.copyOf(node).values();
        }
    }
    
    public static class SimpleTrieBuilder implements Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<SimpleLabelTrie<SimpleTrieBuilder.Node>>> {

        public static SimpleTrieBuilder create() {
            return new SimpleTrieBuilder(SimpleLabelTrie.forRoot(Node.root()));
        }
        
        protected final SimpleLabelTrie<Node> trie;
        
        protected SimpleTrieBuilder(SimpleLabelTrie<Node> trie) {
            this.trie = trie;
        }

        @Override
        public synchronized Optional<SimpleLabelTrie<Node>> apply(
                Optional<? extends SubmittedRequest<Records.Request, ?>> input)
                throws Exception {
            if (input.isPresent()) {
                final Records.Response response = input.get().get().record();
                if (response instanceof Records.ChildrenGetter) {
                    final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.get().request()).getPath());
                    final Node parent = trie.get(path);
                    for (String child: ((Records.ChildrenGetter) response).getChildren()) {
                        ZNodeLabel label = ZNodeLabel.fromString(child);
                        Node existing = parent.put(label, Node.child(label, parent));
                        checkState(existing == null);
                    }
                }
                return Optional.absent();
            } else {
                return Optional.of(trie);
            }
        }
        
        public static final class Node extends AbstractNameTrie.SimpleNode<Node> {

            public static Node root() {
                return new Node(AbstractNameTrie.<Node>rootPointer());
            }

            public static Node child(ZNodeName name, Node parent) {
                return new Node(SimpleLabelTrie.weakPointer(name, parent));
            }
            
            protected Node(Pointer<? extends Node> parent,
                    Map<ZNodeName, Node> children) {
                super(parent, children);
            }

            protected Node(Pointer<? extends Node> parent) {
                super(parent);
            }

            protected Node(ZNodePath path, Pointer<? extends Node> parent,
                    Map<ZNodeName, Node> children) {
                super(path, parent, children);
            }
        }
    }
}
