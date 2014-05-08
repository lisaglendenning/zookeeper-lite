package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolMessageAutomaton;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;

public final class SimpleSessionExecutor extends AbstractSessionExecutor {

    public static ParameterizedFactory<Session, SimpleSessionExecutor> factory(
            final ScheduledExecutorService scheduler,
            final Supplier<? extends TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>>> server) {
        checkNotNull(scheduler);
        checkNotNull(server);
        return new ParameterizedFactory<Session, SimpleSessionExecutor>() {
            @Override
            public SimpleSessionExecutor get(Session value) {
                Automatons.SynchronizedEventfulAutomaton<ProtocolState, Object,?> automaton =
                        Automatons.createSynchronizedEventful(
                                Automatons.createEventful(
                                        Automatons.createLogging(
                                                LogManager.getLogger(SimpleSessionExecutor.class),
                                                ProtocolMessageAutomaton.asAutomaton(
                                                        ProtocolState.CONNECTED))));
                return new SimpleSessionExecutor(
                        value, 
                        automaton,
                        new StrongConcurrentSet<SessionListener>(), 
                        scheduler, 
                        server.get());
            }
        };
    }

    protected final TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server;
    
    public SimpleSessionExecutor(
            Session session,
            Automatons.EventfulAutomaton<ProtocolState,Object> state,
            IConcurrentSet<SessionListener> listeners,
            ScheduledExecutorService scheduler,
            TaskExecutor<? super SessionOperation.Request<?>, Message.ServerResponse<?>> server) {
        super(session, state, listeners, scheduler);
        this.server = checkNotNull(server);
    }
    
    @Override
    protected ListenableFuture<Message.ServerResponse<?>> doSubmit(Message.ClientRequest<?> request) {
        return server.submit(SessionRequest.of(session.id(), request));
    }
}