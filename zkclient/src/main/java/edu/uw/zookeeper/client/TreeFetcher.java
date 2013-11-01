package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class TreeFetcher<V> implements Actor<ZNodeLabel.Path> {
    
    public static <V> Builder<V> builder() {
        return Builder.defaults();
    }
    
    public static class Parameters {

        public static Parameters defaults() {
            return of(false, false, false, false);
        }
        
        public static Parameters of(boolean watch, boolean getData, boolean getAcl,
                boolean getStat) {
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
        
        public Parameters setWatch(boolean watch) {
            return new Parameters(watch, getData, getAcl, getStat);
        }
        
        public boolean getData() {
            return getData;
        }

        public Parameters setData(boolean getData) {
            return new Parameters(watch, getData, getAcl, getStat);
        }
        
        public boolean getAcl() {
            return getAcl;
        }

        public Parameters setAcl(boolean getAcl) {
            return new Parameters(watch, getData, getAcl, getStat);
        }
        
        public boolean getStat() {
            return getStat;
        }

        public Parameters setStat(boolean getStat) {
            return new Parameters(watch, getData, getAcl, getStat);
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("watch", watch).add("getData", getData).add("getAcl", getAcl).add("getStat", getStat).toString();
        }
    }
    
    public static class Builder<V> implements edu.uw.zookeeper.common.Builder<ListenableFuture<Optional<V>>> {
    
        public static <V> Builder<V> defaults() {
            return new Builder<V>();
        }
        
        public static <V> Processor<Object, Optional<V>> nullResult() {
            return new Processor<Object, Optional<V>>() {
                @Override
                public Optional<V> apply(Object input) throws Exception {
                    return Optional.absent();
                }
            };
        }
        
        protected final ZNodeLabel.Path root;
        protected final Parameters parameters;
        protected final Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result;
        protected final Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator;
        protected final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client;
        protected final Executor executor;
        
        public Builder() {
            this(null, null, null, null, null, null);
        }
        
        protected Builder(
                ZNodeLabel.Path root,
                Parameters parameters,
                Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator,
                Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result,
                ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client, 
                Executor executor) {
            this.root = root;
            this.parameters = parameters;
            this.iterator = iterator;
            this.result = result;
            this.client = client;
            this.executor = executor;
        }
        
        public ZNodeLabel.Path getRoot() {
            return root;
        }
        
        public Builder<V> setRoot(ZNodeLabel.Path root) {
            return newInstance(root, parameters, iterator, result, client, executor);
        }

        public Parameters getParameters() {
            return parameters;
        }
        
        public Builder<V> setParameters(Parameters parameters) {
            return newInstance(root, parameters, iterator, result, client, executor);
        }

        public Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> getIterator() {
            return iterator;
        }
        
        public Builder<V> setIterator(Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator) {
            return newInstance(root, parameters, iterator, result, client, executor);
        }

        public Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> getResult() {
            return result;
        }
        
        public Builder<V> setResult(Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result) {
            return newInstance(root, parameters, iterator, result, client, executor);
        }

        public ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> getClient() {
            return client;
        }

        public Builder<V> setClient(ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client) {
            return newInstance(root, parameters, iterator, result, client, executor);
        }

        public Executor getExecutor() {
            return executor;
        }
        
        public Builder<V> setExecutor(Executor executor) {
            return newInstance(root, parameters, iterator, result, client, executor);
        }

        @Override
        public ListenableFuture<Optional<V>> build() {
            return setDefaults().doBuild();
        }
        
        public Builder<V> setDefaults() {
            checkState(client != null);
            
            if (root == null) {
                return setRoot(getDefaultRoot()).setDefaults();
            }
            if (parameters == null) {
                return setParameters(getDefaultParameters()).setDefaults();
            }
            if (iterator == null) {
                return setIterator(getDefaultIterator()).setDefaults();
            }
            if (result == null) {
                return setResult(getDefaultResult()).setDefaults();
            }
            if (executor == null) {
                return setExecutor(getDefaultExecutor()).setDefaults();
            }
            return this;
        }
        
        protected ListenableFuture<Optional<V>> doBuild() {
            Promise<Optional<V>> promise = LoggingPromise.create(LogManager.getLogger(TreeFetcher.class), SettableFuturePromise.<Optional<V>>create());
            TreeFetcher<V> actor = new TreeFetcher<V>(this, promise);
            actor.send(root);
            return actor.future();
        }

        protected Builder<V> newInstance(    
                ZNodeLabel.Path root,
                Parameters parameters,
                Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> iterator,
                Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result,
                ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client, 
                Executor executor) {
            return new Builder<V>(root, parameters, iterator, result, client, executor);
        }
        
        protected ZNodeLabel.Path getDefaultRoot() {
            return ZNodeLabel.Path.root();
        }

        protected Parameters getDefaultParameters() {
            return Parameters.defaults();
        }
        
        protected Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> getDefaultIterator() {
            return TreeProcessor.create();
        }
        
        protected Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> getDefaultResult() {
            return Builder.<V>nullResult();
        }
        
        protected Executor getDefaultExecutor() {
            return MoreExecutors.sameThreadExecutor();
        }
    }

    public static class TreeProcessor implements Processor<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>, Iterator<ZNodeLabel.Path>> {
    
        public static TreeProcessor create() {
            return new TreeProcessor();
        }
        
        @Override
        public Iterator<ZNodeLabel.Path> apply(Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>> input) throws InterruptedException, ExecutionException {
            Records.Response response = input.second().get().record();
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
            return (ZNodeLabel.Path) ZNodeLabel.joined(parent, input);
        }
    }

    protected final Builder<V> builder;
    protected final AtomicReference<State> state;
    // not thread safe
    protected final ImmutableList<Operations.PathBuilder<? extends Records.Request, ?>> builders;
    // not thread safe
    protected final Set<Pending> pending;
    protected final Result future;
    
    protected TreeFetcher(
            Builder<V> builder,
            Promise<Optional<V>> promise) {
        this.builder = checkNotNull(builder);
        this.state = new AtomicReference<State>(State.WAITING);
        this.future = new Result(promise);
        this.pending = Sets.<Pending>newHashSet();
        this.builders = Parameters.toBuilders(builder.parameters);
        this.future.addListener(this, builder.executor);
    }
    
    public ListenableFuture<Optional<V>> future() {
        return future;
    }
    
    @Override
    public State state() {
        return state.get();
    }

    @Override
    public synchronized boolean send(ZNodeLabel.Path input) {
        if (state() == State.TERMINATED) {
            return false;
        } else {
            try {
                for (Operations.PathBuilder<? extends Records.Request, ?> b: builders) {
                    Records.Request request = b.setPath(input).build();
                    ListenableFuture<? extends Operation.ProtocolResponse<?>> future = builder.client.submit(request);
                    Pending task = new Pending(request, future);
                    pending.add(task);
                    future.addListener(task, builder.executor);
                    if (state() == State.TERMINATED) {
                        future.cancel(true);
                        pending.remove(task);
                        return false;
                    }
                }
            } catch (Exception e) {
                stop();
                return false;
            }
            return true;
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
                future.set(builder.result.apply(Optional.<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>absent()));
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
                    Optional<V> value = builder.result.apply(
                            Optional.<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>of(task));
                    if (value.isPresent()) {
                        future.set(value);
                    } else {
                        Iterator<ZNodeLabel.Path> paths = builder.iterator.apply(task);
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