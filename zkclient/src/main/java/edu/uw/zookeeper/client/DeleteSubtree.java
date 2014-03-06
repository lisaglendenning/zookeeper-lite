package edu.uw.zookeeper.client;

import java.util.Set;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class DeleteSubtree implements AsyncFunction<Optional<Set<AbsoluteZNodePath>>, DeleteSubtree> {

    public static ListenableFuture<DeleteSubtree> forRoot(AbsoluteZNodePath root, ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client) {
        return Futures.transform(
                TreeFetcher.<Set<AbsoluteZNodePath>>builder()
                    .setClient(client)
                    .setResult(new ComputeLeaves())
                    .setRoot(root).build(), new DeleteSubtree(client, root));
    }
    
    protected final ClientExecutor<? super Records.Request, ?, ?> client;
    protected final AbsoluteZNodePath root;
    
    protected DeleteSubtree(ClientExecutor<? super Records.Request, ?, ?> client, AbsoluteZNodePath root) {
        this.client = client;
        this.root = root;
    }

    @Override
    public ListenableFuture<DeleteSubtree> apply(Optional<Set<AbsoluteZNodePath>> result) {
        if (result.isPresent()) {
            DeleteLeaves task = new DeleteLeaves(result.get(), SettableFuturePromise.<DeleteSubtree>create());
            task.run();
            return task;
        } else {
            // TODO
            throw new UnsupportedOperationException();
        }
    }
    
    protected static class ComputeLeaves implements Processor<Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<Set<AbsoluteZNodePath>>> {

        protected final Set<AbsoluteZNodePath> leaves;
        
        public ComputeLeaves() {
            this.leaves = Sets.newHashSet();
        }
        
        @Override
        public synchronized Optional<Set<AbsoluteZNodePath>> apply(
                Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>> input)
                throws Exception {
            if (input.isPresent()) {
                Records.Response response = input.get().second().get().record();
                if (response instanceof Records.ChildrenGetter) {
                    if (((Records.ChildrenGetter) response).getChildren().isEmpty()) {
                        leaves.add(AbsoluteZNodePath.fromString(((Records.PathGetter) input.get().first()).getPath()));
                    }
                }
                return Optional.absent();
            } else {
                return Optional.of(leaves);
            }
        }
    }
        
    protected class DeleteLeaves extends PromiseTask<Set<AbsoluteZNodePath>, DeleteSubtree> implements FutureCallback<AbsoluteZNodePath> {
        
        public DeleteLeaves(Set<AbsoluteZNodePath> task, Promise<DeleteSubtree> promise) {
            super(task, promise);
        }
        
        public synchronized void run() {
            if (task().isEmpty()) {
                set(DeleteSubtree.this);
            } else {
                for (AbsoluteZNodePath p: ImmutableSet.copyOf(task())) {
                    DeleteLeaf operation = new DeleteLeaf(p);
                    operation.run();
                }
            }
        }

        @Override
        public synchronized void onSuccess(AbsoluteZNodePath leaf) {
            task().remove(leaf);
            ZNodePath parent = leaf.parent();
            if (parent.startsWith(root)) {
                boolean empty = true;
                for (ZNodeLabelVector p: task()) {
                    if (p.startsWith(parent)) {
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    task().add((AbsoluteZNodePath) parent);
                    DeleteLeaf operation = new DeleteLeaf((AbsoluteZNodePath) parent);
                    operation.run();
                }
            }
            if (task().isEmpty()) {
                set(DeleteSubtree.this);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        protected class DeleteLeaf implements FutureCallback<Operation.ProtocolResponse<?>> {
            
            protected final AbsoluteZNodePath leaf;
            
            public DeleteLeaf(AbsoluteZNodePath leaf) {
                this.leaf = leaf;
            }
            
            public void run() {
                Futures.addCallback(
                        client.submit(Operations.Requests.delete().setPath(leaf).build()), 
                        this);
            }

            @Override
            public void onSuccess(Operation.ProtocolResponse<?> result) {
                if (result.record().opcode() == OpCode.DELETE) {
                    DeleteLeaves.this.onSuccess(leaf);
                } else {
                    // TODO
                    onFailure(KeeperException.create(((Operation.Error) result.record()).error()));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                DeleteLeaves.this.onFailure(t);
            }
        }
    }
}