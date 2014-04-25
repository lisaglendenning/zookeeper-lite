package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import net.engio.mbassy.common.IConcurrentSet;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.SameThreadExecutor;
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

public abstract class AbstractSessionExecutor<V> implements SessionExecutor, FutureCallback<V>, SessionListener {

    protected final Automaton<ProtocolState,ProtocolState> state;
    protected final IConcurrentSet<SessionListener> listeners;
    protected final Session session;
    protected final TimeOutActor<Message.ClientSession, Void> timer;
    
    protected AbstractSessionExecutor(                
            Session session,
            Automaton<ProtocolState,ProtocolState> state,
            IConcurrentSet<SessionListener> listeners,
            ScheduledExecutorService scheduler) {
        this.session = checkNotNull(session);
        this.state = checkNotNull(state);
        this.listeners = checkNotNull(listeners);
        this.timer = TimeOutActor.create(
                TimeOutParameters.create(session.parameters().timeOut()), 
                scheduler);
        
        new TimeOutListener();
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
    }
    
    @Override
    public void onFailure(Throwable t) {
        if (t instanceof TimeoutException) {
            ProtocolState prevState = state.state();
            ProtocolState nextState = ProtocolState.ERROR;
            if (state.apply(nextState).isPresent()) {
                handleAutomatonTransition(Automaton.Transition.create(prevState, nextState));
                Message.ClientRequest<Records.Request> request = ProtocolRequestMessage.of(0, Records.Requests.getInstance().get(OpCode.CLOSE_SESSION));
                submit(request);
            }
        }
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("session", Session.toString(session().id())).toString();
    }
    
    protected class TimeOutListener implements Runnable {

        public TimeOutListener() {
            timer.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void run() {
            if (timer.isDone()) {
                if (! timer.isCancelled()) {
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
}
