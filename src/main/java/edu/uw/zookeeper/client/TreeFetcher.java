package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ForwardingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Operation.SessionResult;
import edu.uw.zookeeper.protocol.Records.ChildrenRecord;
import edu.uw.zookeeper.protocol.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class TreeFetcher extends AbstractActor<ZNodeLabel.Path, Void> {
    
    public static class QueueWatcher {
    
        protected final BlockingQueue<WatchEvent> queue;
    
        public QueueWatcher() {
            this(new LinkedBlockingQueue<WatchEvent>());
        }
        
        public QueueWatcher(BlockingQueue<WatchEvent> queue) {
            this.queue = queue;
        }
        
        public BlockingQueue<WatchEvent> queue() {
            return queue;
        }
        
        @Subscribe
        public void handleReply(Operation.SessionReply message) throws InterruptedException {
            if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
                IWatcherEvent record = (IWatcherEvent) ((Operation.RecordHolder<?>)message.reply()).asRecord();
                handleEvent(record);
            }
        }
        
        public void handleEvent(IWatcherEvent record) throws InterruptedException {
            WatchEvent event = WatchEvent.of(record);
            queue.put(event);
        }
    }

    public static class SubtreeWatcher extends TreeFetcher.QueueWatcher {
    
        protected final ZNodeLabel.Path root;
    
        public SubtreeWatcher(ZNodeLabel.Path root) {
            super();
            this.root = root;
        }
        
        public SubtreeWatcher(ZNodeLabel.Path root, BlockingQueue<WatchEvent> queue) {
            super(queue);
            this.root = root;
        }
    
        @Override
        public void handleEvent(IWatcherEvent record) throws InterruptedException {
            WatchEvent event = WatchEvent.of(record);
            if (root.prefixOf(event.path())) {
                queue.put(event);
            }
        }
    }

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
    
    public static class Pending extends ForwardingQueue<ListenableFuture<Operation.SessionResult>> {

        public static Pending newInstance() {
            return new Pending(new LinkedBlockingQueue<ListenableFuture<Operation.SessionResult>>());
        }
        
        protected final Queue<ListenableFuture<Operation.SessionResult>> delegate;
        
        public Pending(Queue<ListenableFuture<Operation.SessionResult>> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public Queue<ListenableFuture<SessionResult>> delegate() {
            return delegate;
        }
        
        @Override
        public ListenableFuture<Operation.SessionResult> peek() {
            ListenableFuture<Operation.SessionResult> next = super.peek();
            if ((next != null) && (next.isDone())) {
                return next;
            } else {
                return null;
            }
        }

        @Override
        public synchronized ListenableFuture<Operation.SessionResult> poll() {
            ListenableFuture<Operation.SessionResult> next = peek();
            if (next != null) {
                return super.poll();
            } else {
                return null;
            }
        }
        
        @Override
        public boolean isEmpty() {
            return peek() != null;
        }
        
        @Override
        public void clear() {
            ListenableFuture<Operation.SessionResult> next;
            while ((next = super.poll()) != null) {
                next.cancel(true);
            }
        }
    }
    
    public static TreeFetcher newInstance(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor client,
            Executor executor) throws KeeperException, InterruptedException, ExecutionException {
        Promise<List<WatchEvent>> promise = SettableFuturePromise.create();
        return new TreeFetcher(
                parameters, 
                root, 
                client, 
                promise, 
                Pending.newInstance(), 
                executor,
                AbstractActor.<ZNodeLabel.Path>newQueue(),
                AbstractActor.newState());
    }
    
    protected final ClientExecutor client;
    protected final Parameters parameters;
    protected final ZNodeLabel.Path root;
    protected final Promise<List<WatchEvent>> promise;
    protected final Pending pending;
    protected final TreeFetcher.SubtreeWatcher watcher;
    
    protected TreeFetcher(
            Parameters parameters,
            ZNodeLabel.Path root,
            ClientExecutor client,
            Promise<List<WatchEvent>> promise,
            Pending pending,
            Executor executor, 
            Queue<ZNodeLabel.Path> mailbox,
            AtomicReference<State> state) throws KeeperException, InterruptedException, ExecutionException {
        super(executor, mailbox, state);
        this.parameters = checkNotNull(parameters);
        this.root = checkNotNull(root);
        this.client = checkNotNull(client);
        this.promise = checkNotNull(promise);
        this.pending = checkNotNull(pending);
        
        if (parameters.watch()) {
            // sync first
            Operation.Request request = Operations.Requests.sync().setPath(root).build();
            Operations.unlessError(
                    client.submit(request).get().reply().reply(), 
                    request.toString());
            
            this.watcher = new SubtreeWatcher(root);
            client.register(watcher);
        } else {
            this.watcher = null;
        }
        
        send(root);
    }
    
    public ListenableFuture<List<WatchEvent>> future() {
        return promise;
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
                promise.setException(e);
                stop();
                return;
            }
            
            Operation.Request request = result.request().request();
            Operation.Reply reply = Operations.maybeError(result.reply().reply(), KeeperException.Code.NONODE, request.toString());
            if (((OpCode.GET_CHILDREN == request.opcode())
                    || (OpCode.GET_CHILDREN2 == request.opcode()))
                    && !(reply instanceof Operation.Error)) {
                String path = ((Records.PathHolder) ((Operation.RecordHolder<?>)request).asRecord()).getPath();
                Records.ChildrenHolder responseRecord = (ChildrenRecord) ((Operation.RecordHolder<?>)reply).asRecord();
                for (String child: responseRecord.getChildren()) {
                    send(ZNodeLabel.Path.joined(path, child));
                }
            }
        }
        
        super.runAll();
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
            
            try {
                if (!promise.isDone()) {
                    try {
                        List<WatchEvent> events = ImmutableList.of();
                        if (watcher != null) {
                            // sync to flush watches
                            Operation.Request request = Operations.Requests.sync().setPath(root).build();
                            Operations.unlessError(
                                    client.submit(request).get().reply().reply(), 
                                    request.toString());
                            events = Lists.newLinkedList();
                            watcher.queue().drainTo(events);
                        }
                        promise.set(events);
                    } catch (Exception e) {
                        promise.setException(e);
                    }
                }
            } finally {
                if (watcher != null) {
                    client.unregister(watcher);
                }
            }
        }
        return stopped;
    }
    
    public static class Builder {

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
        
        public TreeFetcher build() throws Exception {
            Parameters parameters = Parameters.of(operations, watch);
            return TreeFetcher.newInstance(parameters, root, client, executor);
        }
    }
}