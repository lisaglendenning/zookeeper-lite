package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ForwardingQueue;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class TreeFetcher implements Callable<ListenableFuture<ZNodeLabel.Path>> {
    
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
    
    public static class Builder {
    
        public static Builder create() {
            return new Builder();
        }
        
        protected volatile ZNodeLabel.Path root;
        protected volatile ClientExecutor client;
        protected volatile Executor executor;
        protected volatile Set<OpCode> operations; 
        protected volatile boolean watch;
        
        public Builder() {
            this(ZNodeLabel.Path.root(), 
                    null, MoreExecutors.sameThreadExecutor(), EnumSet.noneOf(OpCode.class), false);
        }
        
        public Builder(
                ZNodeLabel.Path root, 
                ClientExecutor client, 
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
    
        public Builder setRoot(ZNodeLabel.Path root) {
            this.root = root;
            return this;
        }
        
        public ClientExecutor getClient() {
            return client;
        }
    
        public Builder setClient(ClientExecutor client) {
            this.client = client;
            return this;
        }
        
        public Executor getExecutor() {
            return executor;
        }
    
        public Builder setExecutor(Executor executor) {
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
        
        public Builder setStat(boolean getStat) {
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
        
        public Builder setData(boolean getData) {
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
        
        public Builder setAcl(boolean getAcl) {
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
        
        public Builder setWatch(boolean watch) {
            this.watch = watch;
            return this;
        }
        
        public TreeFetcher build() {
            Parameters parameters = Parameters.of(operations, watch);
            return TreeFetcher.newInstance(parameters, root, client, executor);
        }
    }

    public static TreeFetcher newInstance(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor client,
            Executor executor) {
        Promise<ZNodeLabel.Path> promise = SettableFuturePromise.create();
        return newInstance(
                parameters, 
                root, 
                client, 
                executor,
                promise);
    }

    public static TreeFetcher newInstance(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor client,
            Executor executor,
            Promise<ZNodeLabel.Path> promise) {
        return new TreeFetcher(
                parameters, 
                root, 
                client, 
                promise, 
                executor);
    }
    
    protected final Executor executor;
    protected final ClientExecutor client;
    protected final Parameters parameters;
    protected final ZNodeLabel.Path root;
    protected final Promise<ZNodeLabel.Path> promise;
    
    protected TreeFetcher(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor client,
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
        TreeFetcherActor actor = newActor();
        actor.send(root);
        return actor.future();
    }
    
    protected TreeFetcherActor newActor() {
        return TreeFetcherActor.newInstance(parameters, root, client, executor, promise);
    }

    public static class TreeFetcherActor extends AbstractActor<ZNodeLabel.Path, Void> {

        public static TreeFetcherActor newInstance(
                Parameters parameters,
                ZNodeLabel.Path root,
                ClientExecutor client,
                Executor executor,
                Promise<ZNodeLabel.Path> promise) {
            return new TreeFetcherActor(
                    promise, 
                    parameters, 
                    root, 
                    client, 
                    executor);
        }
        
        protected final ClientExecutor client;
        protected final Parameters parameters;
        protected final Task task;
        protected final Pending<Operation.SessionResult, ListenableFuture<Operation.SessionResult>> pending;
        
        protected TreeFetcherActor(
                Promise<ZNodeLabel.Path> promise,
                Parameters parameters,
                ZNodeLabel.Path root,
                ClientExecutor client,
                Executor executor) {
            super(executor, AbstractActor.<ZNodeLabel.Path>newQueue(), AbstractActor.newState());
            this.parameters = checkNotNull(parameters);
            this.client = checkNotNull(client);
            this.task = new Task(root, promise);
            this.pending = Pending.newInstance();
        }
        
        public ZNodeLabel.Path root() {
            return task.task();
        }

        public ListenableFuture<ZNodeLabel.Path> future() {
            return task;
        }

        @Override
        protected Void apply(ZNodeLabel.Path input) throws Exception {
            Operations.Requests.GetChildren getChildrenBuilder = 
                    Operations.Requests.getChildren().setPath(input).setWatch(parameters.watch());
            if (parameters.getStat()) {
                getChildrenBuilder.setStat(true);
            }
            ListenableFuture<Operation.SessionResult> future = client.submit(getChildrenBuilder.build());
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
            
            return null;
        }

        @Override
        public boolean runEnter() {
            if (State.WAITING == state.get()) {
                schedule();
                return false;
            } else {
                return super.runEnter();
            }
        }

        @Override
        protected void runAll() throws Exception {
            // process pending first
            ListenableFuture<Operation.SessionResult> future;
            while ((future = pending.poll()) != null) {
                Operation.SessionResult result;
                try {
                    result = future.get();
                } catch (Exception e) {
                    task.setException(e);
                    stop();
                    return;
                }
                
                handleResult(result);
            }
            
            super.runAll();
        }
        
        protected void handleResult(Operation.SessionResult result) throws Exception {
            Operation.Request request = result.request().request();
            Operation.Response reply = Operations.maybeError(result.reply().reply(), KeeperException.Code.NONODE, request.toString());
            if (reply instanceof Records.ChildrenHolder) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathHolder) request).getPath());
                for (String child: ((Records.ChildrenHolder) reply).getChildren()) {
                    send(ZNodeLabel.Path.of(path, ZNodeLabel.Component.of(child)));
                }
            }
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
        public boolean stop() {
            boolean stopped = super.stop();
            
            if (stopped) {
                pending.clear();
                task.complete();
            }
            return stopped;
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

    public static class Pending<U,T extends Future<U>> extends ForwardingQueue<T> {
    
        public static <U,T extends Future<U>> Pending<U,T> newInstance() {
            return new Pending<U,T>(new LinkedBlockingQueue<T>());
        }
        
        protected final Queue<T> delegate;
        
        public Pending(Queue<T> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public Queue<T> delegate() {
            return delegate;
        }
        
        @Override
        public T peek() {
            T next = super.peek();
            if ((next != null) && (next.isDone())) {
                return next;
            } else {
                return null;
            }
        }
    
        @Override
        public synchronized T poll() {
            T next = peek();
            if (next != null) {
                return super.poll();
            } else {
                return null;
            }
        }
        
        @Override
        public boolean isEmpty() {
            return peek() == null;
        }
        
        @Override
        public void clear() {
            T next;
            while ((next = super.poll()) != null) {
                next.cancel(true);
            }
        }
    }
}