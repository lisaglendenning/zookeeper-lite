package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import net.engio.mbassy.common.IConcurrentSet;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.SessionExecutor;

public abstract class AbstractSessionExecutor implements SessionExecutor, FutureCallback<Message.ServerResponse<?>>, SessionListener {

    protected final Automatons.EventfulAutomaton<ProtocolState, Object> state;
    protected final IConcurrentSet<SessionListener> listeners;
    protected final Session session;
    protected final TimeOutActor<Message.ClientSession, Void> timer;
    
    protected AbstractSessionExecutor(                
            Session session,
            Automatons.EventfulAutomaton<ProtocolState, Object> state,
            IConcurrentSet<SessionListener> listeners,
            ScheduledExecutorService scheduler) {
        this.session = checkNotNull(session);
        this.state = checkNotNull(state);
        this.listeners = checkNotNull(listeners);
        this.timer = TimeOutActor.create(
                TimeOutParameters.milliseconds(session.parameters().timeOut().value(TimeUnit.MILLISECONDS)), 
                scheduler,
                LogManager.getLogger(this));
        
        new TimeOutListener();
        state.subscribe(this);
        timer.run();
    }
    
    public TimeOutActor<Message.ClientSession, Void> timer() {
        return timer;
    }

    @Override
    public ProtocolState state() {
        return state.state();
    }

    @Override
    public Session session() {
        return session;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Message.ClientRequest<?> request) {
        timer.send(request);
        state.apply(request);
        ListenableFuture<Message.ServerResponse<?>> future = doSubmit(request);
        new SubmitListener(future);
        return future;
    }
    
    @Override
    public void subscribe(SessionListener listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return listeners.remove(listener);
    }

    @Override
    public void handleNotification(
            Operation.ProtocolResponse<IWatcherEvent> notification) {
        for (SessionListener listener: listeners) {
            listener.handleNotification(notification);
        }
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        for (SessionListener listener: listeners) {
            listener.handleAutomatonTransition(transition);
        }

        switch (transition.to()) {
        case ERROR:
            // close session on error
            Message.ClientRequest<Records.Request> request = ProtocolRequestMessage.of(0, Records.Requests.getInstance().get(OpCode.CLOSE_SESSION));
            submit(request);
            break;
        case DISCONNECTED:
            timer.cancel(false);
            break;
        default:
            break;
        }
    }

    @Override
    public void onSuccess(Message.ServerResponse<?> result) {
        try {
            state.apply(result);
        } catch (IllegalArgumentException e) {}
    }
    
    @Override
    public void onFailure(Throwable t) {
        state.apply(ProtocolState.ERROR);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("session", Session.toString(session().id())).toString();
    }
    
    protected abstract ListenableFuture<Message.ServerResponse<?>> doSubmit(Message.ClientRequest<?> request);
    
    protected class TimeOutListener implements Runnable {

        public TimeOutListener() {
            timer.addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public void run() {
            if (timer.isDone()) {
                if (!timer.isCancelled()) {
                    try {
                        timer.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        onFailure(e.getCause());
                    }
                }
            }
        }
    }
    
    protected class SubmitListener implements Runnable {

        private final ListenableFuture<Message.ServerResponse<?>> future;
        
        public SubmitListener(ListenableFuture<Message.ServerResponse<?>> future) {
            this.future = future;
            future.addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public void run() {
            if (future.isDone()) {
                if (!future.isCancelled()) {
                    Message.ServerResponse<?> result;
                    try {
                        result = future.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        onFailure(e.getCause());
                        return;
                    }
                    onSuccess(result);
                }
            }
        }
    }
}
