package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class TreeFetcher<U extends Operation.ProtocolResponse<?>, V> implements AsyncFunction<ZNodeLabel.Path, Optional<V>> {
    
    public static <U extends Operation.ProtocolResponse<?>, V> Builder<U,V> builder() {
        return Builder.create();
    }
    
    public static class Parameters {
        
        public static Parameters of(Set<OpCode> operations, boolean watch) {
            boolean getData = operations.contains(OpCode.GET_DATA);
            boolean getAcl = operations.contains(OpCode.GET_ACL);
            boolean getStat = operations.contains(OpCode.EXISTS) 
                    || operations.contains(OpCode.GET_CHILDREN2);
            return new Parameters(watch, getData, getAcl, getStat);
        }
        
        public static ImmutableList<Operations.PathBuilder<? extends Records.Request, ?>> toBuilders(Parameters parameters) {
            ImmutableList.Builder<Operations.PathBuilder<? extends Records.Request, ?>> builders = ImmutableList.builder();
            builders.add(
                    Operations.Requests.getChildren()
                    .setWatch(parameters.getWatch()).setStat(parameters.getStat()));
            if (parameters.getData()) {
                builders.add(
                        Operations.Requests.getData().setWatch(parameters.getWatch()));
            }
            if (parameters.getAcl()) {
                builders.add(
                        Operations.Requests.getAcl());
            }
            return builders.build();
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
    
    public static class Builder<U extends Operation.ProtocolResponse<?>, V> {
    
        public static <U extends Operation.ProtocolResponse<?>, V> Builder<U,V> create() {
            return new Builder<U,V>();
        }
        
        public static <V> Processor<Object, Optional<V>> nullResult() {
            return new Processor<Object, Optional<V>>() {
                @Override
                public Optional<V> apply(Object input) throws Exception {
                    return Optional.absent();
                }
            };
        }
        
        protected volatile ClientExecutor<? super Records.Request, U> client;
        protected volatile Set<OpCode> operations; 
        protected volatile boolean watch;
        protected volatile Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result;
        protected volatile Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator;
        
        public Builder() {
            this(null, 
                    EnumSet.noneOf(OpCode.class), 
                    false,
                    TreeProcessor.create(),
                    Builder.<V>nullResult());
        }
        
        public Builder(
                ClientExecutor<? super Records.Request, U> client, 
                Set<OpCode> operations, 
                boolean watch,
                Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator,
                Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result) {
            this.client = client;
            this.watch = watch;
            this.operations = Collections.synchronizedSet(operations);
            this.iterator = iterator;
            this.result = result;
        }
        
        public ClientExecutor<? super Records.Request, U> getClient() {
            return client;
        }
    
        public Builder<U,V> setClient(ClientExecutor<? super Records.Request, U> client) {
            this.client = client;
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
        
        public Builder<U,V> setStat(boolean getStat) {
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
        
        public Builder<U,V> setData(boolean getData) {
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
        
        public Builder<U,V> setAcl(boolean getAcl) {
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
        
        public Builder<U,V> setWatch(boolean watch) {
            this.watch = watch;
            return this;
        }
        
        public Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> getIterator() {
            return iterator;
        }
        
        public Builder<U,V> setIterator(Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator) {
            this.iterator = iterator;
            return this;
        }

        public Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> getResult() {
            return result;
        }
        
        public Builder<U,V> setResult(Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result) {
            this.result = result;
            return this;
        }
        
        public TreeFetcher<U,V> build() {
            Parameters parameters = Parameters.of(operations, watch);
            return TreeFetcher.newInstance(parameters, client, iterator, result);
        }
    }

    public static class TreeProcessor implements Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> {
    
        public static TreeProcessor create() {
            return new TreeProcessor();
        }
        
        @Override
        public Iterator<ZNodeLabel.Path> apply(Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>> input) throws InterruptedException, ExecutionException {
            Records.Response response = input.second().get().getRecord();
            if (response instanceof Records.ChildrenGetter) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input.first()).getPath());
                return Iterators.transform(
                        ((Records.ChildrenGetter) response).getChildren().iterator(),
                        new ChildToPath(path));
            } else {
                return Iterators.emptyIterator();
            }
        }
    }

    public static class ChildToPath implements Function<String, ZNodeLabel.Path> {
        
        private final ZNodeLabel.Path parent;
        
        public ChildToPath(ZNodeLabel.Path parent) {
            this.parent = parent;
        }
        
        @Override
        public ZNodeLabel.Path apply(String input) {
            return ZNodeLabel.Path.of(parent, ZNodeLabel.Component.of(input));
        }
    }

    public static <U extends Operation.ProtocolResponse<?>, V> TreeFetcher<U,V> newInstance(
            Parameters parameters,
            ClientExecutor<? super Records.Request, U> client,
            Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator,
            Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result) {
        return new TreeFetcher<U,V>(
                parameters,
                client, 
                iterator,
                result);
    }

    protected final Parameters parameters;
    protected final ClientExecutor<? super Records.Request, U> client;
    protected final Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator;
    protected final Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result;
    
    protected TreeFetcher(
            Parameters parameters,
            ClientExecutor<? super Records.Request, U> client,
            Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator,
            Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result) {
        this.parameters = checkNotNull(parameters);
        this.client = checkNotNull(client);
        this.iterator = checkNotNull(iterator);
        this.result = checkNotNull(result);
    }

    @Override
    public ListenableFuture<Optional<V>> apply(ZNodeLabel.Path root) {
        TreeFetcherActor actor = new TreeFetcherActor(SettableFuturePromise.<Optional<V>>create());
        actor.send(root);
        return actor.future();
    }
    
    protected class TreeFetcherActor implements Actor<ZNodeLabel.Path> {

        protected final AtomicReference<State> state;
        // not thread safe
        protected final ImmutableList<Operations.PathBuilder<? extends Records.Request, ?>> builders;
        // not thread safe
        protected final Set<Pending> pending;
        protected final Result future;
        
        public TreeFetcherActor(
                Promise<Optional<V>> promise) {
            this.state = new AtomicReference<State>(State.WAITING);
            this.future = new Result(promise);
            this.pending = Sets.<Pending>newHashSet();
            this.builders = Parameters.toBuilders(parameters);
            this.future.addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        public ListenableFuture<Optional<V>> future() {
            return future;
        }
        
        @Override
        public State state() {
            return state.get();
        }

        @Override
        public synchronized void send(ZNodeLabel.Path input) {
            if (state() != State.TERMINATED) {
                try {
                    for (Operations.PathBuilder<? extends Records.Request, ?> b: builders) {
                        Records.Request request = b.setPath(input).build();
                        ListenableFuture<? extends Operation.ProtocolResponse<?>> future = client.submit(request);
                        Pending task = new Pending(request, future);
                        pending.add(task);
                        future.addListener(task, MoreExecutors.sameThreadExecutor());
                        if (state() == State.TERMINATED) {
                            future.cancel(true);
                            pending.remove(task);
                            break;
                        }
                    }
                } catch (Exception e) {
                    stop();
                }
            }
        }
        
        @Override
        public synchronized void run() {
            if (state() != State.TERMINATED) {
                if (future.isDone() || pending.isEmpty()) {
                    // We're done!
                    stop();
                }
            }
        }
        
        @Override
        public synchronized boolean stop() {
            boolean stopped = (state.get() != State.TERMINATED)
                    && (state.getAndSet(State.TERMINATED) != State.TERMINATED);
            if (stopped) {
                Iterator<Pending> itr = Iterators.consumingIterator(pending.iterator());
                while (itr.hasNext()) {
                    itr.next().second().cancel(true);
                }
                try {
                    future.set(result.apply(Optional.<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>absent()));
                } catch (Exception e) {
                    future.setException(e);
                }
            }
            return stopped;
        }

        protected synchronized void handlePending(Pending task) {
            if (task.second().isDone()) {
                if (state() != State.TERMINATED) {
                    try {
                        Optional<V> value = result.apply(
                                Optional.<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>of(task));
                        if (value.isPresent()) {
                            future.set(value);
                        } else {
                            Iterator<ZNodeLabel.Path> paths = iterator.apply(task);
                            while (paths.hasNext()) {
                                send(paths.next());
                            }
                        }
                    } catch (Exception e) {
                        future.setException(e);
                    }
                }
                pending.remove(task);
                run();
            }
        }

        protected class Pending extends Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>> implements Runnable {

            public Pending(Records.Request request, ListenableFuture<? extends Operation.ProtocolResponse<?>> future) {
                super(request, future);
            }
            
            @Override
            public void run() {
                handlePending(this);
            }
        }
        
        protected class Result extends ForwardingPromise<Optional<V>> {

            protected final Promise<Optional<V>> delegate;
            
            protected Result(Promise<Optional<V>> delegate) {
                this.delegate = delegate;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean canceled = super.cancel(mayInterruptIfRunning);
                if (canceled) {
                    stop();
                }
                return canceled;
            }

            @Override
            protected Promise<Optional<V>> delegate() {
                return delegate;
            }
        }
    }
}