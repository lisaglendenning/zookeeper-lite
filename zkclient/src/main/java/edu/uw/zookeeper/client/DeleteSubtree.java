package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.DefaultsNode.AbstractDefaultsNode;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class DeleteSubtree extends ForwardingListenableFuture<AbsoluteZNodePath> implements Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<DeleteSubtree>>, FutureCallback<DeleteSubtree.DeleteLeaf>, Runnable {

    public static ListenableFuture<List<AbsoluteZNodePath>> deleteChildren(
            final ZNodePath parent, 
            final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client) {
        return Futures.transform(
                GetChildren.create(
                        parent, 
                        client, 
                        SettableFuturePromise.<List<AbsoluteZNodePath>>create()), 
                new AsyncFunction<List<AbsoluteZNodePath>, List<AbsoluteZNodePath>>() {
                    @Override
                    public ListenableFuture<List<AbsoluteZNodePath>> apply(
                            List<AbsoluteZNodePath> children) throws Exception {
                        ImmutableList.Builder<ListenableFuture<AbsoluteZNodePath>> deletes = ImmutableList.builder();
                        for (AbsoluteZNodePath child: children) {
                            deletes.add(DeleteSubtree.deleteAll(child, client));
                        }
                        return Futures.allAsList(deletes.build());
                    }
                });
    }
    
    public static ListenableFuture<AbsoluteZNodePath> deleteAll(
            AbsoluteZNodePath path, 
            ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client) {
        final DeleteSubtree delete = new DeleteSubtree(client, path, SettableFuturePromise.<AbsoluteZNodePath>create());
        final ListenableFuture<Optional<DeleteSubtree>> fetcher = TreeWalker.forResult(delete)
                    .setClient(client)
                    .setResult(delete)
                    .setRequests(TreeWalker.toRequests(TreeWalker.parameters().setSync(true)))
                    .setRoot(path).build();
        fetcher.addListener(
                new Runnable() {
                    @Override
                    public void run() {
                        if (fetcher.isCancelled()) {
                            delete.cancel(false);
                        }
                    }
                }, MoreExecutors.directExecutor());
        return Futures.transform(
                fetcher,
                new AsyncFunction<Optional<DeleteSubtree>,AbsoluteZNodePath>() {
                    @Override
                    public ListenableFuture<AbsoluteZNodePath> apply(
                            Optional<DeleteSubtree> input) throws Exception {
                        return input.get();
                    }
                });
    }
    
    protected final SimpleLabelTrie<Node> trie;
    protected final PathToQuery<?,?> delete;
    protected final ClientExecutor<? super Records.Request, ?, ?> client;
    protected final AbsoluteZNodePath root;
    protected final Set<DeleteLeaf> deletes;
    protected final Promise<AbsoluteZNodePath> promise;
    
    @SuppressWarnings("unchecked")
    protected DeleteSubtree(
            ClientExecutor<? super Records.Request, ?, ?> client, 
            AbsoluteZNodePath root,
            Promise<AbsoluteZNodePath> promise) {
        this.trie = SimpleLabelTrie.forRoot(Node.root());
        this.client = client;
        this.root = root;
        this.delete = PathToQuery.forRequests(client, Operations.Requests.delete());
        this.deletes = Sets.newHashSet();
        this.promise = promise;
        addListener(this, MoreExecutors.directExecutor());
    }

    @Override
    public synchronized void run() {
        if (isDone()) {
            for (DeleteLeaf leaf: Iterables.consumingIterable(deletes)) {
                leaf.cancel(false);
            }
            trie.clear();
        }
    }

    @Override
    public synchronized Optional<DeleteSubtree> apply(
            Optional<? extends SubmittedRequest<Records.Request,?>> input)
            throws Exception {
        final Optional<DeleteSubtree> result;
        if (input.isPresent()) {
            final Records.Response response = input.get().get().record();
            if (response instanceof Records.ChildrenGetter) {
                final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.get().request()).getPath());
                final Node parent = Node.putIfAbsent(trie, path);
                for (String child: ((Records.ChildrenGetter) response).getChildren()) {
                    parent.putIfAbsent(ZNodeLabel.fromString(child));
                }
                if (parent.isEmpty()) {
                    delete(parent);
                }
            }
            result = Optional.absent();
        } else {
            result = Optional.of(this);
        }
        return result;
    }


    @Override
    public synchronized void onSuccess(DeleteLeaf result) {
        checkArgument(result.isDone());
        deletes.remove(this);
        if (!result.isCancelled()) {
            try {
                for (Operation.ProtocolResponse<?> response: result.get()) {
                    Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                }
            } catch (Exception e) {
                onFailure(e);
            }
            Node parent = result.node().parent().get();
            if (result.node().remove() && (parent != null) && (parent.isEmpty())) {
                if (parent.path().length() >= root.length()) {
                    delete(parent);
                } else {
                    // done!
                    promise.set(root);
                }
            }
        }
    }

    @Override
    public void onFailure(Throwable t) {
        if (!isDone()) {
            promise.setException(t);
        }
    }
    
    protected DeleteLeaf delete(Node node) {
        DeleteLeaf listener = new DeleteLeaf(node, Futures.<Operation.ProtocolResponse<?>>allAsList(delete.apply(node.path()).call()));
        deletes.add(listener);
        listener.addListener(listener, MoreExecutors.directExecutor());
        return listener;
    }
    
    @Override
    protected ListenableFuture<AbsoluteZNodePath> delegate() {
        return promise;
    }

    protected static class GetChildren implements Callable<Optional<List<AbsoluteZNodePath>>> {

        public static ListenableFuture<List<AbsoluteZNodePath>> create(
                ZNodePath path,
                ClientExecutor<? super Records.Request,?,?> client, 
                Promise<List<AbsoluteZNodePath>> promise) {
            SubmittedRequests<Records.Request,?> requests = 
                    SubmittedRequests.submitRequests(
                            client, 
                            Operations.Requests.sync().setPath(path).build(), 
                            Operations.Requests.getChildren().setPath(path).build());
            CallablePromiseTask<GetChildren,List<AbsoluteZNodePath>> task = CallablePromiseTask.create(new GetChildren(requests), promise);
            requests.addListener(task, MoreExecutors.directExecutor());
            return task;
        }
        
        protected final SubmittedRequests<Records.Request,?> requests;
        
        public GetChildren(
                SubmittedRequests<Records.Request,?> requests) {
            this.requests = requests;
        }
        
        @Override
        public Optional<List<AbsoluteZNodePath>> call() throws Exception {
            if (requests.isDone()) {
                List<? extends Operation.ProtocolResponse<?>> responses = requests.get();
                ImmutableList.Builder<AbsoluteZNodePath> children = ImmutableList.builder();
                for (int i=0; i<requests.requests().size(); ++i) {
                    Operation.ProtocolResponse<?> response = responses.get(i);
                    if (!Operations.maybeError(response.record(), KeeperException.Code.NONODE).isPresent()) {
                        if (response.record() instanceof Records.ChildrenGetter) {
                            ZNodePath prefix = ZNodePath.fromString(((Records.PathGetter) requests.requests().get(i)).getPath());
                            for (String child: ((Records.ChildrenGetter) response.record()).getChildren()) {
                                children.add((AbsoluteZNodePath) prefix.join(ZNodeLabel.fromString(child)));
                            }
                        }
                    }
                }
                return Optional.<List<AbsoluteZNodePath>>of(children.build());
            } else {
                return Optional.absent();
            }
        }
    }
    
    protected static final class Node extends AbstractDefaultsNode<Node> {

        public static Node root() {
            return new Node(AbstractNameTrie.<Node>rootPointer());
        }

        public static Node child(ZNodeName name, Node parent) {
            NameTrie.Pointer<Node> childPointer = SimpleLabelTrie.weakPointer(name, parent);
            return new Node(childPointer);
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

        @Override
        protected Node newChild(ZNodeName label) {
            return child(label, this);
        }
    }

    protected final class DeleteLeaf extends ForwardingListenableFuture<List<Operation.ProtocolResponse<?>>> implements Runnable {

        private final Node node;
        private final ListenableFuture<List<Operation.ProtocolResponse<?>>> future;
        
        public DeleteLeaf(Node node, ListenableFuture<List<Operation.ProtocolResponse<?>>> future) {
            this.node = node;
            this.future = future;
        }
        
        public Node node() {
            return node;
        }
        
        @Override
        public void run() {
            if (isDone()) {
                onSuccess(this);
            }
        }

        @Override
        protected ListenableFuture<List<Operation.ProtocolResponse<?>>> delegate() {
            return future;
        }
    }
}