package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.FutureQueue;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class TreeFetcher<T extends Operation.ProtocolRequest<Records.Request>, U extends Operation.ProtocolResponse<Records.Response>, V> implements AsyncFunction<ZNodeLabel.Path, V> {
    
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
        
        public boolean getWatch() {
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
    
    public static class Builder<T extends Operation.ProtocolRequest<Records.Request>, U extends Operation.ProtocolResponse<Records.Response>, V> {
    
        public static <T extends Operation.ProtocolRequest<Records.Request>, U extends Operation.ProtocolResponse<Records.Response>, V> Builder<T,U,V> create() {
            return new Builder<T,U,V>();
        }
        
        public static <V> Callable<V> nullResult() {
            return new Callable<V>() {
                @Override
                public V call() throws Exception {
                    return null;
                }
            };
        }
        
        protected volatile ClientExecutor<Operation.Request, T, U> client;
        protected volatile Executor executor;
        protected volatile Set<OpCode> operations; 
        protected volatile boolean watch;
        protected volatile Callable<V> result;
        
        public Builder() {
            this(null, 
                    MoreExecutors.sameThreadExecutor(), 
                    EnumSet.noneOf(OpCode.class), 
                    false,
                    Builder.<V>nullResult());
        }
        
        public Builder(
                ClientExecutor<Operation.Request, T, U> client, 
                Executor executor,
                Set<OpCode> operations, 
                boolean watch,
                Callable<V> result) {
            this.client = client;
            this.watch = watch;
            this.operations = Collections.synchronizedSet(operations);
            this.result = result;
        }
        
        public ClientExecutor<Operation.Request, T, U> getClient() {
            return client;
        }
    
        public Builder<T,U,V> setClient(ClientExecutor<Operation.Request, T, U> client) {
            this.client = client;
            return this;
        }
        
        public Executor getExecutor() {
            return executor;
        }
    
        public Builder<T,U,V> setExecutor(Executor executor) {
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
        
        public Builder<T,U,V> setStat(boolean getStat) {
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
        
        public Builder<T,U,V> setData(boolean getData) {
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
        
        public Builder<T,U,V> setAcl(boolean getAcl) {
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
        
        public Builder<T,U,V> setWatch(boolean watch) {
            this.watch = watch;
            return this;
        }

        public Callable<V> getResult() {
            return result;
        }
        
        public Builder<T,U,V> setResult(Callable<V> result) {
            this.result = result;
            return this;
        }
        
        public TreeFetcher<T,U,V> build() {
            Parameters parameters = Parameters.of(operations, watch);
            return TreeFetcher.newInstance(parameters, client, executor, result);
        }
    }

    public static <T extends Operation.ProtocolRequest<Records.Request>, U extends Operation.ProtocolResponse<Records.Response>, V> TreeFetcher<T,U,V> newInstance(
            Parameters parameters,
            ClientExecutor<Operation.Request, T, U> client,
            Executor executor,
            Callable<V> result) {
        return new TreeFetcher<T,U,V>(
                parameters,
                client, 
                result,
                executor);
    }
    
    protected final Executor executor;
    protected final ClientExecutor<Operation.Request, T, U> client;
    protected final Parameters parameters;
    protected final Callable<V> result;
    
    protected TreeFetcher(
            Parameters parameters,
            ClientExecutor<Operation.Request, T, U> client,
            Callable<V> result,
            Executor executor) {
        this.parameters = checkNotNull(parameters);
        this.client = checkNotNull(client);
        this.result = checkNotNull(result);
        this.executor = checkNotNull(executor);
    }

    
    @Override
    public ListenableFuture<V> apply(ZNodeLabel.Path root) {
        TreeFetcherActor<T,U,V> actor = newActor();
        actor.send(root);
        return actor.future();
    }
    
    protected TreeFetcherActor<T,U,V> newActor() {
        Promise<V> promise = SettableFuturePromise.create();
        return TreeFetcherActor.newInstance(parameters, client, result, executor, promise);
    }

    public static class TreeFetcherActor<T extends Operation.ProtocolRequest<Records.Request>, U extends Operation.ProtocolResponse<Records.Response>, V> extends AbstractActor<ZNodeLabel.Path> {

        public static <T extends Operation.ProtocolRequest<Records.Request>, U extends Operation.ProtocolResponse<Records.Response>, V> TreeFetcherActor<T,U,V> newInstance(
                Parameters parameters,
                ClientExecutor<Operation.Request, T, U> client,
                Callable<V> result,
                Executor executor,
                Promise<V> promise) {
            return new TreeFetcherActor<T,U,V>(
                    promise, 
                    parameters,
                    client,
                    result,
                    executor);
        }
        
        protected final ClientExecutor<Operation.Request, T, U> client;
        protected final FutureQueue<ListenableFuture<Pair<T,U>>> pending;
        protected final Task task;
        protected final Operations.Requests.GetChildren getChildrenBuilder;
        protected final Operations.Requests.GetData getDataBuilder;
        protected final Operations.Requests.GetAcl getAclBuilder;
        
        protected TreeFetcherActor(
                Promise<V> promise,
                Parameters parameters,
                ClientExecutor<Operation.Request, T, U> client,
                Callable<V> result,
                Executor executor) {
            super(executor, AbstractActor.<ZNodeLabel.Path>newQueue(), AbstractActor.newState());
            this.client = checkNotNull(client);
            this.task = new Task(result, promise);
            this.pending = FutureQueue.create();
            this.getChildrenBuilder = 
                    Operations.Requests.getChildren().setWatch(parameters.getWatch()).setStat(parameters.getStat());
            this.getDataBuilder = parameters.getData()
                    ? Operations.Requests.getData().setWatch(parameters.getWatch())
                            : null;
            this.getAclBuilder = parameters.getAcl()
                    ? Operations.Requests.getAcl() 
                            : null;
        }
        
        public ListenableFuture<V> future() {
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
            ListenableFuture<Pair<T,U>> future;
            while ((future = pending.poll()) != null) {
                Pair<T,U> result;
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

        protected void applyPendingResult(Pair<T,U> result) throws Exception {
            Records.Request request = result.first().getRecord();
            Records.Response response = result.second().getRecord();
            Operations.maybeError(response, KeeperException.Code.NONODE, request.toString());
            if (response instanceof Records.ChildrenGetter) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
                for (String child: ((Records.ChildrenGetter) response).getChildren()) {
                    send(ZNodeLabel.Path.of(path, ZNodeLabel.Component.of(child)));
                }
            }
        }

        @Override
        protected boolean apply(ZNodeLabel.Path input) throws Exception {
            ListenableFuture<Pair<T,U>> future = 
                    client.submit(getChildrenBuilder.setPath(input).build());
            pending.add(future);
            future.addListener(this, executor);
            
            if (getDataBuilder != null) {
                future = client.submit(
                        getDataBuilder.setPath(input).build());
                pending.add(future);
                future.addListener(this, executor);
            }
                
            if (getAclBuilder != null) {
                future = client.submit(
                        getAclBuilder.setPath(input).build());
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
        
        protected class Task extends PromiseTask<Callable<V>, V> {

            protected Task(Callable<V> task, Promise<V> delegate) {
                super(task, delegate);
            }
            
            protected boolean complete() {
                boolean doComplete = ! isDone();
                if (doComplete) {
                    try {
                        doComplete = set(task().call());
                    } catch (Throwable t) {
                        doComplete = setException(t);
                    }
                }
                return doComplete;
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