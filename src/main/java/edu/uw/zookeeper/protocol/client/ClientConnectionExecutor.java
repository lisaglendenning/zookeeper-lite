package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ClientConnectionExecutor<C extends Connection<? super Message.ClientSession>>
    extends AbstractActor<PromiseTask<Operation.Request, Pair<Message.ClientRequest, Message.ServerResponse>>, Void>
    implements ClientExecutor<Operation.Request, Message.ClientRequest, Message.ServerResponse>,
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
        return new ClientConnectionExecutor<C>(
                request,
                connection,
                xids,
                executor);
    }
    
    protected final C connection;
    protected final ConnectTask connectTask;
    protected final AssignXidProcessor xids;
    protected final BlockingQueue<PendingTask> pending;
    protected final BlockingQueue<Message.ServerResponse> received;
    
    protected ClientConnectionExecutor(
            ConnectMessage.Request request,
            C connection,
            AssignXidProcessor xids,
            Executor executor) {
        super(executor, AbstractActor.<PromiseTask<Operation.Request, Pair<Message.ClientRequest, Message.ServerResponse>>>newQueue(), AbstractActor.newState());
        this.connection = connection;
        this.xids = xids;
        this.pending = new LinkedBlockingQueue<PendingTask>();
        this.received = new LinkedBlockingQueue<Message.ServerResponse>();
        this.connectTask = ConnectTask.create(connection, request);
                
        register(this);
    }

    public Session session() {
        if (connectTask.isDone()) {
            try {
                return connectTask.get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            return Session.uninitialized();
        }
    }
    
    @Override
    public C get() {
        return connection;
    }
    
    @Override
    public ListenableFuture<Pair<Message.ClientRequest, Message.ServerResponse>> submit(Operation.Request request) {
        Promise<Pair<Message.ClientRequest, Message.ServerResponse>> promise = SettableFuturePromise.create();
        return submit(request, promise);
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest, Message.ServerResponse>> submit(Operation.Request request, Promise<Pair<Message.ClientRequest, Message.ServerResponse>> promise) {
        PromiseTask<Operation.Request, Pair<Message.ClientRequest, Message.ServerResponse>> task = PromiseTask.of(request, promise);
        send(task);
        return task;
    }

    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            stop();
        }
    }

    @Subscribe
    public void handleResponse(Message.ServerResponse message) throws InterruptedException {
        if ((state.get() != State.TERMINATED) && !pending.isEmpty()) {
            received.put(message);
            schedule();
        }
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
    
    @Override
    public boolean stop() {
        boolean isStopped = super.stop();
        if (isStopped) {
            try {
                unregister(this);
            } catch (IllegalArgumentException e) {}
            
            PendingTask next = null;
            while ((next = pending.poll()) != null) {
                next.cancel(true);
            }
            received.clear();
        }
        return isStopped;
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
    protected void runAll() throws Exception {
        PendingTask next = null;
        while ((next = pending.peek()) != null) {
            Message.ServerResponse response = null;
            while ((response = received.poll()) != null) {
                if (next.task().xid() == response.xid()) {
                    next.set(Pair.create(next.task(), response));
                }
            }
            if (next.isDone()) {
                pending.remove(next);
            } else {
                break;
            }
        }
        
        super.runAll();
    }

    @Override
    protected Void apply(PromiseTask<Operation.Request, Pair<Message.ClientRequest, Message.ServerResponse>> input) {
        PendingTask task;
        try {
            Message.ClientRequest message = (Message.ClientRequest) xids.apply(input.task());
            task = new PendingTask(message, input);
        } catch (Throwable t) {
            input.setException(t);
            return null;
        }
    
        try {
            // task needs to be in the queue before calling write
            pending.add(task);
            ListenableFuture<Message.ClientRequest> future = connection.write(task.task());
            Futures.addCallback(future, task);
        } catch (Throwable t) {
            task.setException(t);
        } finally {
            task.addListener(this, executor);
        }
        
        return null;
    }

    @Override
    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            if (! mailbox.isEmpty()) {
                schedule();
            } else if (! pending.isEmpty() && (pending.peek().isDone() || ! received.isEmpty())) {
                schedule();
            }
        }
    }
    
    protected static class PendingTask
        extends PromiseTask<Message.ClientRequest, Pair<Message.ClientRequest, Message.ServerResponse>>
        implements FutureCallback<Message.ClientRequest> {
    
        protected PendingTask(
                Message.ClientRequest task,
                Promise<Pair<Message.ClientRequest, Message.ServerResponse>> delegate) {
            super(task, delegate);
        }
    
        @Override
        public void onSuccess(Message.ClientRequest result) {
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    } 
}
