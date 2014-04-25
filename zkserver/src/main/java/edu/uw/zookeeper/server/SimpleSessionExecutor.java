package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;

public class SimpleSessionExecutor extends AbstractSessionExecutor<Object> {

    public static ParameterizedFactory<Session, SimpleSessionExecutor> factory(
            final ScheduledExecutorService scheduler,
            final Supplier<? extends TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>>> server) {
        checkNotNull(scheduler);
        checkNotNull(server);
        return new ParameterizedFactory<Session, SimpleSessionExecutor>() {
            @Override
            public SimpleSessionExecutor get(Session value) {
                return new SimpleSessionExecutor(
                        value, 
                        Automatons.createSynchronized(
                                Automatons.createSimple(
                                        ProtocolState.CONNECTED)),
                        new StrongConcurrentSet<SessionListener>(), 
                        scheduler, 
                        server.get());
            }
        };
    }

    protected final TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server;
    
    public SimpleSessionExecutor(
            Session session,
            Automaton<ProtocolState,ProtocolState> state,
            IConcurrentSet<SessionListener> listeners,
            ScheduledExecutorService scheduler,
            TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server) {
        super(session, state, listeners, scheduler);
        this.server = checkNotNull(server);
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Message.ClientRequest<?> request) {
        timer.send(request);
        return server.submit(SessionRequest.of(session.id(), request));
    }

    @Override
    public void onSuccess(Object result) {
    }
}