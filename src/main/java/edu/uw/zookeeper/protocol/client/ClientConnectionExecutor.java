package edu.uw.zookeeper.protocol.client;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ClientConnectionExecutor<C extends Connection<? super Message.ClientSession>>
    extends AbstractActor<PromiseTask<Operation.Request, Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>>>
    implements ClientExecutor<Operation.Request, Message.ClientRequest<?>, Message.ServerResponse<?>>,
        Publisher,
        Reference<C> {

    public static <C extends Connection<? super Message.ClientSession>> ClientConnectionExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection) {
        return newInstance(
                request,
                connection,
                AssignXidProcessor.newInstance(),
                connection);
    }

    public static <C extends Connection<? super Message.ClientSession>> ClientConnectionExecutor<C> newInstance(
            ConnectMessage.Request request,
            Executor executor,
            AssignXidProcessor xids,
            C connection) {
        return newInstance(
                ConnectTask.create(connection, request),
                executor,
                xids,
                connection);
    }

    public static <C extends Connection<? super Message.ClientSession>> ClientConnectionExecutor<C> newInstance(
            ListenableFuture<ConnectMessage.Response> session,
            Executor executor,
            AssignXidProcessor xids,
            C connection) {
        return new ClientConnectionExecutor<C>(
                session,
                connection,
                xids,
                executor);
    }
    
    protected final C connection;
    protected final ListenableFuture<ConnectMessage.Response> session;
    protected final AssignXidProcessor xids;
    protected final Queue<PendingTask> pending;
    protected final Queue<Message.ServerResponse<?>> received;
    
    protected ClientConnectionExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            AssignXidProcessor xids,
            Executor executor) {
        super(executor, AbstractActor.<PromiseTask<Operation.Request, Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>>>newQueue(), AbstractActor.newState());
        this.connection = connection;
        this.xids = xids;
        this.pending = new ConcurrentLinkedQueue<PendingTask>();
        this.received = new ConcurrentLinkedQueue<Message.ServerResponse<?>>();
        this.session = session;
                
        connection.register(this);
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    @Override
    public C get() {
        return connection;
    }
    
    @Override
    public ListenableFuture<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> submit(Operation.Request request) {
        Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> promise = SettableFuturePromise.create();
        return submit(request, promise);
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> submit(
            Operation.Request request, Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> promise) {
        PromiseTask<Operation.Request, Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> task = PromiseTask.of(request, promise);
        send(task);
        return task;
    }

    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Override
    public void post(Object object) {
        get().post(object);
    }

    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            stop();
        }
    }

    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) throws InterruptedException {
        if ((state.get() != State.TERMINATED) && !pending.isEmpty()) {
            receive(message);
        }
    }
    
    protected void receive(Message.ServerResponse<?> message) throws InterruptedException {
        // ignore pings
        if (message.getXid() != OpCodeXid.PING.getXid()) {
            received.add(message);
            schedule();
        }
    }

    @Override
    protected boolean runEnter() {
        if (state.get() == State.WAITING) {
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
    
    protected void doPending() {
        PendingTask task = null;
        while (((task = pending.peek()) != null) 
                || !received.isEmpty()) {
            Message.ServerResponse<?> response = null;
            while (((task == null) || !task.isDone()) 
                    && ((response = received.poll()) != null)) {
                applyReceived(task, response);
            }
            if (task != null) {
                if (task.isDone()) {
                    pending.remove(task);
                } else {
                    break;
                }
            }
        }
    }
    
    protected void applyReceived(PendingTask task, Message.ServerResponse<?> response) {
        if ((task != null) && (task.task().getXid() == response.getXid())) {
            Pair<Message.ClientRequest<?>, Message.ServerResponse<?>> result = 
                    Pair.<Message.ClientRequest<?>, Message.ServerResponse<?>>create(task.task(), response);
            task.set(result);
        }
    }

    @Override
    protected boolean apply(PromiseTask<Operation.Request, Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> input) {
        PendingTask task;
        try {
            Message.ClientRequest<?> message = (Message.ClientRequest<?>) xids.apply(input.task());
            task = new PendingTask(message, input);
        } catch (Throwable t) {
            input.setException(t);
            return true;
        }
    
        try {
            // task needs to be in the queue before calling write
            pending.add(task);
            ListenableFuture<?> future = get().write(task.task());
            Futures.addCallback(future, task);
        } catch (Throwable t) {
            task.setException(t);
        } finally {
            task.addListener(this, executor);
        }
        
        return true;
    }

    @Override
    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            if (! mailbox.isEmpty()
                    || (! pending.isEmpty() 
                            && (pending.peek().isDone() 
                                    || ! received.isEmpty()))) {
                schedule();
            }
        }
    }
    
    @Override
    protected void doStop() {
        super.doStop();
    
        try {
            get().unregister(this);
        } catch (IllegalArgumentException e) {}
        
        if (! session.isDone()) {
            session.cancel(true);
        }
        
        get().close();
        
        PendingTask next = null;
        while ((next = pending.poll()) != null) {
            next.cancel(true);
        }
        received.clear();
    }

    protected static class PendingTask
        extends PromiseTask<Message.ClientRequest<?>, Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>>
        implements FutureCallback<Object> {
    
        public PendingTask(
                Message.ClientRequest<?> task,
                Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> delegate) {
            super(task, delegate);
        }
    
        @Override
        public void onSuccess(Object result) {
            // mark pings as done on write because ZooKeeper doesn't care about their ordering
            assert (task() == result);
            if (task().getXid() == OpCodeXid.PING.getXid()) {
                set(null);
            }
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        @Override
        public Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> delegate() {
            return delegate;
        }
    } 
}
