package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.RootZNodePath;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class TreeWalker<V> extends AbstractActor<ZNodePath> implements ListenableFuture<Optional<V>> {
    
    public static <V> Builder<V> builder() {
        return Builder.defaults();
    }

    public static <V> Builder<V> forResult(Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<V>> result) {
        return Builder.<V>defaults().setResult(result);
    }
    
    public static Parameters parameters() {
        return Parameters.defaults();
    }
    
    public static PathToRequests toRequests(Parameters parameters) {
        ImmutableList.Builder<Operations.Builder<? extends Records.Request>> builders = ImmutableList.builder();
        if (parameters.getSync()) {
            builders.add(Operations.Requests.sync());
        }
        if (parameters.getData()) {
            builders.add(
                    Operations.Requests.getData().setWatch(parameters.getWatch()));
        }
        if (parameters.getAcl()) {
            builders.add(
                    Operations.Requests.getAcl());
        }
        builders.add(
                Operations.Requests.getChildren()
                .setWatch(parameters.getWatch()).setStat(parameters.getStat()));
        return PathToRequests.forIterable(builders.build());
    }
    
    public static <V> Processor<Object, Optional<V>> noResult() {
        return new Processor<Object, Optional<V>>() {
            @Override
            public Optional<V> apply(Object input) {
                return Optional.absent();
            }
        };
    }
    
    public static final class Parameters {

        public static Parameters defaults() {
            return valueOf(true, false, false, false, false);
        }
        
        public static Parameters valueOf(boolean sync, boolean watch, boolean getData, boolean getAcl,
                boolean getStat) {
            return new Parameters(sync, watch, getData, getAcl, getStat);
        }
        
        private final boolean sync;
        private final boolean watch;
        private final boolean getData;
        private final boolean getAcl;
        private final boolean getStat;
        
        protected Parameters(boolean sync, boolean watch, boolean getData, boolean getAcl,
                boolean getStat) {
            this.sync = sync;
            this.watch = watch;
            this.getData = getData;
            this.getAcl = getAcl;
            this.getStat = getStat;
        }
        
        public boolean getSync() {
            return sync;
        }
        
        public Parameters setSync(boolean sync) {
            return valueOf(sync, watch, getData, getAcl, getStat);
        }
        
        public boolean getWatch() {
            return watch;
        }
        
        public Parameters setWatch(boolean watch) {
            return valueOf(sync, watch, getData, getAcl, getStat);
        }
        
        public boolean getData() {
            return getData;
        }

        public Parameters setData(boolean getData) {
            return valueOf(sync, watch, getData, getAcl, getStat);
        }
        
        public boolean getAcl() {
            return getAcl;
        }

        public Parameters setAcl(boolean getAcl) {
            return valueOf(sync, watch, getData, getAcl, getStat);
        }
        
        public boolean getStat() {
            return getStat;
        }

        public Parameters setStat(boolean getStat) {
            return valueOf(sync, watch, getData, getAcl, getStat);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("sync", sync)
                    .add("watch", watch)
                    .add("getData", getData)
                    .add("getAcl", getAcl)
                    .add("getStat", getStat).toString();
        }
    }
    
    public static final class Builder<V> implements edu.uw.zookeeper.common.Builder<TreeWalker<V>> {
    
        public static <V> Builder<V> defaults() {
            return new Builder<V>(ZNodePath.root(), null, null, null, null, null);
        }
        
        private final ZNodePath root;
        private final PathToRequests requests;
        private final Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<V>> result;
        private final Processor<? super SubmittedRequest<Records.Request,?>, ? extends Iterator<? extends ZNodePath>> iterator;
        private final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client;
        private final Executor executor;
        
        protected Builder(
                ZNodePath root,
                PathToRequests requests,
                Processor<? super SubmittedRequest<Records.Request,?>, ? extends Iterator<? extends ZNodePath>> iterator,
                Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<V>> result,
                ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client, 
                Executor executor) {
            this.root = root;
            this.requests = requests;
            this.iterator = iterator;
            this.result = result;
            this.client = client;
            this.executor = executor;
        }
        
        public ZNodePath getRoot() {
            return root;
        }
        
        public Builder<V> setRoot(ZNodePath root) {
            return newInstance(root, requests, iterator, result, client, executor);
        }

        public PathToRequests getRequests() {
            return requests;
        }
        
        public Builder<V> setRequests(PathToRequests requests) {
            return newInstance(root, requests, iterator, result, client, executor);
        }

        public Processor<? super SubmittedRequest<Records.Request,?>, ? extends Iterator<? extends ZNodePath>> getIterator() {
            return iterator;
        }
        
        public Builder<V> setIterator(Processor<? super SubmittedRequest<Records.Request,?>, ? extends Iterator<? extends ZNodePath>> iterator) {
            return newInstance(root, requests, iterator, result, client, executor);
        }

        public Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<V>> getResult() {
            return result;
        }
        
        public <U> Builder<U> setResult(Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<U>> result) {
            return newInstance(root, requests, iterator, result, client, executor);
        }

        public ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> getClient() {
            return client;
        }

        public Builder<V> setClient(ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client) {
            return newInstance(root, requests, iterator, result, client, executor);
        }

        public Executor getExecutor() {
            return executor;
        }
        
        public Builder<V> setExecutor(Executor executor) {
            return newInstance(root, requests, iterator, result, client, executor);
        }

        @Override
        public TreeWalker<V> build() {
            return setDefaults().doBuild();
        }
        
        public Builder<V> setDefaults() {
            checkState(client != null);
            
            if (root == null) {
                return setRoot(getDefaultRoot()).setDefaults();
            }
            if (requests == null) {
                return setRequests(getDefaultRequests()).setDefaults();
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
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("root", getRoot())
                    .add("requests", getRequests())
                    .add("iterator", getIterator())
                    .add("result", getResult())
                    .add("client", getClient())
                    .add("executor", getExecutor()).toString();
        }
        
        protected TreeWalker<V> doBuild() {
            Promise<Optional<V>> promise = SettableFuturePromise.<Optional<V>>create();
            TreeWalker<V> actor = new TreeWalker<V>(this, promise);
            actor.send(getRoot());
            return actor;
        }

        protected <U> Builder<U> newInstance(    
                ZNodePath root,
                PathToRequests requests,
                Processor<? super SubmittedRequest<Records.Request,?>, ? extends Iterator<? extends ZNodePath>> iterator,
                Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<U>> result,
                ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client, 
                Executor executor) {
            return new Builder<U>(root, requests, iterator, result, client, executor);
        }
        
        protected ZNodePath getDefaultRoot() {
            return RootZNodePath.getInstance();
        }

        protected PathToRequests getDefaultRequests() {
            return toRequests(parameters());
        }
        
        protected Processor<? super SubmittedRequest<Records.Request,?>, ? extends Iterator<? extends ZNodePath>> getDefaultIterator() {
            return GetChildrenIterator.create();
        }
        
        protected Processor<? super Optional<? extends SubmittedRequest<Records.Request,?>>, ? extends Optional<V>> getDefaultResult() {
            return TreeWalker.<V>noResult();
        }
        
        protected Executor getDefaultExecutor() {
            return MoreExecutors.directExecutor();
        }
    }

    public static final class GetChildrenIterator implements Processor<SubmittedRequest<Records.Request,?>, Iterator<AbsoluteZNodePath>> {
    
        public static GetChildrenIterator create() {
            return new GetChildrenIterator();
        }
        
        protected GetChildrenIterator() {}
        
        @Override
        public Iterator<AbsoluteZNodePath> apply(SubmittedRequest<Records.Request,?> input) throws InterruptedException, ExecutionException {
            final Records.Response response = input.get().record();
            if (response instanceof Records.ChildrenGetter) {
                final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.getValue()).getPath());
                return Iterators.transform(
                                ((Records.ChildrenGetter) response).getChildren().iterator(),
                                ChildToPath.forParent(path));
            } else {
                return ImmutableSet.<AbsoluteZNodePath>of().iterator();
            }
        }
    }

    // not thread safe
    protected final Builder<V> builder;
    protected final Queue<SubmittedRequest<Records.Request,?>> pending;
    
    protected final Promise<Optional<V>> future;
    
    protected TreeWalker(
            Builder<V> builder,
            Promise<Optional<V>> promise) {
        super(LogManager.getLogger(TreeWalker.class));
        this.builder = checkNotNull(builder);
        this.future = promise;
        this.pending = Queues.newArrayDeque();
        
        addListener(this, builder.getExecutor());
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public Optional<V> get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public Optional<V> get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        future.addListener(listener, executor);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(builder.getRoot()).addValue(ToStringListenableFuture.toString(future)).toString();
    }

    @Override
    protected synchronized boolean doSend(ZNodePath message) {
        List<Records.Request> requests = builder.getRequests().apply(message);
        List<SubmittedRequest<Records.Request,?>> submits = Lists.newArrayListWithCapacity(requests.size());
        for (Records.Request request: requests) {
            SubmittedRequest<Records.Request,?> submitted = SubmittedRequest.submit(builder.getClient(), request);
            submits.add(submitted);
            pending.add(submitted);
            if (state() == State.TERMINATED) {
                for (SubmittedRequest<Records.Request,?> e: submits) {
                    e.cancel(false);
                    pending.remove(e);
                }
                return false;
            }
        }
        for (SubmittedRequest<Records.Request,?> submitted: submits) {
            submitted.addListener(new RequestListener(submitted), builder.getExecutor());
        }
        return true;
    }

    @Override
    protected synchronized void doRun() throws Exception {
        if (isDone() || pending.isEmpty()) {
            // We're done!
            stop();
        }
    }

    @Override
    protected synchronized void doStop() {
        Iterator<? extends Future<?>> itr = Iterators.consumingIterator(pending.iterator());
        while (itr.hasNext()) {
            itr.next().cancel(true);
        }
        try {
            future.set(builder.getResult().apply(Optional.<SubmittedRequest<Records.Request,?>>absent()));
        } catch (Exception e) {
            future.setException(e);
        }
    }

    protected synchronized void handleRequest(SubmittedRequest<Records.Request,?> request) {
        assert (request.isDone());
        pending.remove(request);
        if (!isDone() && (state() != State.TERMINATED)) {
            try {
                Optional<V> value = builder.getResult().apply(Optional.of(request));
                if (value.isPresent()) {
                    future.set(value);
                } else {
                    Iterator<? extends ZNodePath> paths = builder.getIterator().apply(request);
                    while (paths.hasNext()) {
                        send(paths.next());
                    }
                }
            } catch (Exception e) {
                future.setException(e);
            }
            run();
        }
    }

    protected class RequestListener implements Runnable {

        protected final SubmittedRequest<Records.Request,?> future;
        
        public RequestListener(SubmittedRequest<Records.Request,?> future) {
            this.future = future;
        }
        
        @Override
        public void run() {
            if (future.isDone()) {
                handleRequest(future);
            }
        }
    }
}
