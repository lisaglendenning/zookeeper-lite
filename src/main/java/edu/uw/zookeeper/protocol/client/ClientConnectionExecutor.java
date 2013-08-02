package edu.uw.zookeeper.protocol.client;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;

public class ClientConnectionExecutor<C extends Connection<? super Message.ClientSession>>
    extends AbstractActor<PromiseTask<Operation.Request, Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>>>
    implements ClientExecutor<Operation.Request, Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>,
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
    protected final Queue<Message.ServerResponse<Records.Response>> received;
    
    protected ClientConnectionExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            AssignXidProcessor xids,
            Executor executor) {
        super(executor, AbstractActor.<PromiseTask<Operation.Request, Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>>>newQueue(), AbstractActor.newState());
        this.connection = connection;
        this.xids = xids;
        this.pending = new ConcurrentLinkedQueue<PendingTask>();
        this.received = new ConcurrentLinkedQueue<Message.ServerResponse<Records.Response>>();
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
    public ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> submit(Operation.Request request) {
        Promise<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> promise = SettableFuturePromise.create();
        return submit(request, promise);
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> submit(
            Operation.Request request, Promise<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> promise) {
        PromiseTask<Operation.Request, Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> task = PromiseTask.of(request, promise);
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
    public void handleResponse(Message.ServerResponse<Records.Response> message) throws InterruptedException {
        if ((state.get() != State.TERMINATED) && !pending.isEmpty()) {
            receive(message);
        }
    }
    
    protected void receive(Message.ServerResponse<Records.Response> message) throws InterruptedException {
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
            Message.ServerResponse<Records.Response> response = null;
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
    
    protected void applyReceived(PendingTask task, Message.ServerResponse<Records.Response> response) {
        if ((task != null) && (task.task().getXid() == response.getXid())) {
            Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> result = 
                    Pair.<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>create(task.task(), response);
            task.set(result);
        }
    }

    @Override
    protected boolean apply(PromiseTask<Operation.Request, Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> input) {
        PendingTask task;
        try {
            @SuppressWarnings("unchecked")
            Message.ClientRequest<Records.Request> message = (Message.ClientRequest<Records.Request>) xids.apply(input.task());
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
        extends PromiseTask<Message.ClientRequest<Records.Request>, Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>>
        implements FutureCallback<Object> {
    
        public PendingTask(
                Message.ClientRequest<Records.Request> task,
                Promise<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> delegate) {
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
        public Promise<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> delegate() {
            return delegate;
        }
    } 
}
