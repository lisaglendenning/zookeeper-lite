package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class TreeFetcher<T extends Operation.SessionRequest, V extends Operation.SessionResponse> implements Callable<ListenableFuture<ZNodeLabel.Path>> {
    
    public static class Parameters {
        
        public static Parameters of(Set<OpCode> operations, boolean watch) {
            boolean getData = operations.contains(OpCode.GET_DATA);
            boolean getAcl = operations.contains(OpCode.GET_ACL);
            boolean getStat = operations.contains(OpCode.EXISTS) 
                    || operations.contains(OpCode.GET_CHILDREN2);
            return new Parameters(watch, getData, getAcl, getStat);
        }
        
        protected final boolean watch;
        protected final boolean getData;
        protected final boolean getAcl;
        protected final boolean getStat;
        
        public Parameters(boolean watch, boolean getData, boolean getAcl,
                boolean getStat) {
            super();
            this.watch = watch;
            this.getData = getData;
            this.getAcl = getAcl;
            this.getStat = getStat;
        }
        
        public boolean watch() {
            return watch;
        }
        
        public boolean getData() {
            return getData;
        }
        
        public boolean getAcl() {
            return getAcl;
        }
        
        public boolean getStat() {
            return getStat;
        }
    }
    
    public static class Builder<T extends Operation.SessionRequest, V extends Operation.SessionResponse> {
    
        public static <T extends Operation.SessionRequest, V extends Operation.SessionResponse> Builder<T,V> create() {
            return new Builder<T,V>();
        }
        
        protected volatile ZNodeLabel.Path root;
        protected volatile ClientExecutor<Operation.Request, T, V> client;
        protected volatile Executor executor;
        protected volatile Set<OpCode> operations; 
        protected volatile boolean watch;
        
        public Builder() {
            this(ZNodeLabel.Path.root(), 
                    null, MoreExecutors.sameThreadExecutor(), EnumSet.noneOf(OpCode.class), false);
        }
        
        public Builder(
                ZNodeLabel.Path root, 
                ClientExecutor<Operation.Request, T, V> client, 
                Executor executor,
                Set<OpCode> operations, 
                boolean watch) {
            this.root = root;
            this.client = client;
            this.watch = watch;
            this.operations = Collections.synchronizedSet(operations);
        }
        
        public ZNodeLabel.Path getRoot() {
            return root;
        }
    
        public Builder<T,V> setRoot(ZNodeLabel.Path root) {
            this.root = root;
            return this;
        }
        
        public ClientExecutor<Operation.Request, T, V> getClient() {
            return client;
        }
    
        public Builder<T,V> setClient(ClientExecutor<Operation.Request, T, V> client) {
            this.client = client;
            return this;
        }
        
        public Executor getExecutor() {
            return executor;
        }
    
        public Builder<T,V> setExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }
    
        public boolean getStat() {
            OpCode[] ops = { OpCode.EXISTS, OpCode.GET_CHILDREN2 };
            for (OpCode op: ops) {
                if (operations.contains(op)) {
                    return true;
                }
            }
            return false;
        }
        
        public Builder<T,V> setStat(boolean getStat) {
            OpCode[] ops = { OpCode.EXISTS, OpCode.GET_CHILDREN2 };
            if (getStat) {
                for (OpCode op: ops) {
                    operations.add(op);
                }
            } else {
                for (OpCode op: ops) {
                    operations.remove(op);
                }
            }
            return this;
        }
    
        public boolean getData() {
            OpCode op = OpCode.GET_DATA;
            return operations.contains(op);
        }
        
        public Builder<T,V> setData(boolean getData) {
            OpCode op = OpCode.GET_DATA;
            if (getData) {
                operations.add(op);
            } else {
                operations.remove(op);
            }
            return this;
        }
    
        public boolean getAcl() {
            OpCode op = OpCode.GET_ACL;
            return operations.contains(op);
        }
        
        public Builder<T,V> setAcl(boolean getAcl) {
            OpCode op = OpCode.GET_ACL;
            if (getAcl) {
                operations.add(op);
            } else {
                operations.remove(op);
            }
            return this;
        }
        
        public boolean getWatch() {
            return watch;
        }
        
        public Builder<T,V> setWatch(boolean watch) {
            this.watch = watch;
            return this;
        }
        
        public TreeFetcher<T,V> build() {
            Parameters parameters = Parameters.of(operations, watch);
            return TreeFetcher.newInstance(parameters, root, client, executor);
        }
    }

    public static <T extends Operation.SessionRequest, V extends Operation.SessionResponse> TreeFetcher<T,V> newInstance(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor<Operation.Request, T, V> client,
            Executor executor) {
        Promise<ZNodeLabel.Path> promise = SettableFuturePromise.create();
        return newInstance(
                parameters, 
                root, 
                client, 
                executor,
                promise);
    }

    public static <T extends Operation.SessionRequest, V extends Operation.SessionResponse> TreeFetcher<T,V> newInstance(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor<Operation.Request, T, V> client,
            Executor executor,
            Promise<ZNodeLabel.Path> promise) {
        return new TreeFetcher<T,V>(
                parameters, 
                root, 
                client, 
                promise, 
                executor);
    }
    
    protected final Executor executor;
    protected final ClientExecutor<Operation.Request, T, V> client;
    protected final Parameters parameters;
    protected final ZNodeLabel.Path root;
    protected final Promise<ZNodeLabel.Path> promise;
    
    protected TreeFetcher(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor<Operation.Request, T, V> client,
            Promise<ZNodeLabel.Path> promise,
            Executor executor) {
        this.parameters = checkNotNull(parameters);
        this.root = checkNotNull(root);
        this.client = checkNotNull(client);
        this.promise = checkNotNull(promise);
        this.executor = checkNotNull(executor);
    }

    
    @Override
    public ListenableFuture<ZNodeLabel.Path> call() {
        TreeFetcherActor<T,V> actor = newActor();
        actor.send(root);
        return actor.future();
    }
    
    protected TreeFetcherActor<T,V> newActor() {
        return TreeFetcherActor.newInstance(parameters, root, client, executor, promise);
    }

    public static class TreeFetcherActor<T extends Operation.SessionRequest, V extends Operation.SessionResponse> extends AbstractActor<ZNodeLabel.Path> {

        public static <T extends Operation.SessionRequest, V extends Operation.SessionResponse> TreeFetcherActor<T,V> newInstance(
                Parameters parameters,
                ZNodeLabel.Path root,
                ClientExecutor<Operation.Request, T, V> client,
                Executor executor,
                Promise<ZNodeLabel.Path> promise) {
            return new TreeFetcherActor<T,V>(
                    promise, 
                    parameters, 
                    root, 
                    client, 
                    executor);
        }
        
        protected final ClientExecutor<Operation.Request, T, V> client;
        protected final Parameters parameters;
        protected final Task task;
        protected final FutureQueue<ListenableFuture<Pair<T,V>>> pending;
        
        protected TreeFetcherActor(
                Promise<ZNodeLabel.Path> promise,
                Parameters parameters,
                ZNodeLabel.Path root,
                ClientExecutor<Operation.Request, T, V> client,
                Executor executor) {
            super(executor, AbstractActor.<ZNodeLabel.Path>newQueue(), AbstractActor.newState());
            this.parameters = checkNotNull(parameters);
            this.client = checkNotNull(client);
            this.task = new Task(root, promise);
            this.pending = FutureQueue.create();
        }
        
        public ZNodeLabel.Path root() {
            return task.task();
        }

        public ListenableFuture<ZNodeLabel.Path> future() {
            return task;
        }

        @Override
        protected boolean runEnter() {
            if (State.WAITING == state.get()) {
                schedule();
                return false;
            } else {
                return super.runEnter();
            }
        }

        @Override
        protected void doRun() throws Exception {
            doPending();
            
            super.doRun();
        }

        protected void doPending() throws Exception {
            ListenableFuture<Pair<T,V>> future;
            while ((future = pending.poll()) != null) {
                Pair<T,V> result;
                try {
                    result = future.get();
                } catch (Exception e) {
                    task.setException(e);
                    stop();
                    return;
                }
                
                applyPendingResult(result);
            }
        }

        protected void applyPendingResult(Pair<T,V> result) throws Exception {
            Records.Request request = result.first().request();
            Records.Response reply = Operations.maybeError(result.second().response(), KeeperException.Code.NONODE, request.toString());
            if (reply instanceof Records.ChildrenGetter) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
                for (String child: ((Records.ChildrenGetter) reply).getChildren()) {
                    send(ZNodeLabel.Path.of(path, ZNodeLabel.Component.of(child)));
                }
            }
        }

        @Override
        protected boolean apply(ZNodeLabel.Path input) throws Exception {
            Operations.Requests.GetChildren getChildrenBuilder = 
                    Operations.Requests.getChildren().setPath(input).setWatch(parameters.watch());
            if (parameters.getStat()) {
                getChildrenBuilder.setStat(true);
            }
            ListenableFuture<Pair<T,V>> future = client.submit(getChildrenBuilder.build());
            pending.add(future);
            future.addListener(this, executor);
            
            if (parameters.getData()) {
                future = client.submit(
                        Operations.Requests.getData()
                        .setPath(input).setWatch(parameters.watch()).build());
                pending.add(future);
                future.addListener(this, executor);
            }
                
            if (parameters.getAcl()) {
                future = client.submit(Operations.Requests.getAcl()
                        .setPath(input).build());
                pending.add(future);
                future.addListener(this, executor);
            }
            
            return true;
        }

        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                if (!mailbox.isEmpty() || !pending.isEmpty()) {
                    schedule();
                } else if (pending.delegate().isEmpty()) {
                    // We're done!
                    stop();
                }
            }
        }
        
        @Override
        protected void doStop() {
            super.doStop();
            
            pending.clear();
            task.complete();
        }
        
        protected class Task extends PromiseTask<ZNodeLabel.Path, ZNodeLabel.Path> {

            protected Task(ZNodeLabel.Path task, Promise<ZNodeLabel.Path> delegate) {
                super(task, delegate);
            }
            
            protected boolean complete() {
                boolean completed = ! isDone();
                if (completed) {
                    completed = set(task());
                }
                return completed;
            }
            
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean canceled = super.cancel(mayInterruptIfRunning);
                if (canceled) {
                    stop();
                }
                return canceled;
            }
        }
    }
}