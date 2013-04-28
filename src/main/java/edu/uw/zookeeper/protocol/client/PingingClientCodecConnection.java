package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.OpPing;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionRequestWrapper;
import edu.uw.zookeeper.util.AutomatonTransition;
import edu.uw.zookeeper.util.EventfulAutomaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.TimeValue;

public class PingingClientCodecConnection extends ClientCodecConnection implements Runnable {

    public static Factory<? extends ClientCodecConnection> factory(
            Factory<Publisher> publisherFactory,
            Factory<Connection> connectionFactory,
            TimeValue defaultTimeOut,
            ScheduledExecutorService executor) {
        return factory(connectionFactory,
                Builder.newInstance(publisherFactory, defaultTimeOut, executor));
    }
    
    public static class Builder implements ParameterizedFactory<Connection, PingingClientCodecConnection> {
    
        public static Builder newInstance(
                Factory<Publisher> publisherFactory,
                TimeValue defaultTimeOut,
                ScheduledExecutorService executor) {
            return new Builder(publisherFactory, defaultTimeOut, executor);
        }
        
        private final ScheduledExecutorService executor;
        private final TimeValue defaultTimeOut;
        private final Factory<Publisher> publisherFactory;
    
        private Builder(
                Factory<Publisher> publisherFactory,
                TimeValue defaultTimeOut,
                ScheduledExecutorService executor) {
            super();
            this.executor = executor;
            this.defaultTimeOut = defaultTimeOut;
            this.publisherFactory = publisherFactory;
        }
    
        @Override
        public PingingClientCodecConnection get(Connection connection) {
            Publisher publisher = publisherFactory.get();
            return PingingClientCodecConnection.newInstance(
                    publisher, connection, executor, defaultTimeOut);
        }
    }

    public static PingingClientCodecConnection newInstance(
            Publisher publisher,
            Connection connection,
            ScheduledExecutorService executor,
            TimeValue timeOut) {
        EventfulAutomaton<ProtocolState, Message> automaton = EventfulAutomaton.createSynchronized(publisher, ProtocolState.ANONYMOUS);
        ClientProtocolCodec codec = ClientProtocolCodec.create(automaton);
        PingingClientCodecConnection client = newInstance(publisher, codec, connection, executor, timeOut);
        automaton.register(client);
        return client;
    }

    protected static PingingClientCodecConnection newInstance(
            Publisher publisher,
            ClientProtocolCodec codec,
            Connection connection,
            ScheduledExecutorService executor,
            TimeValue timeOut) {
        return new PingingClientCodecConnection(publisher, codec, connection, executor, timeOut);
    }

    private static enum PingingState {
        WAITING, SCHEDULED, STOPPED;
    }
    
    private static long now() {
        return System.currentTimeMillis();
    }
    
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final Logger logger = LoggerFactory
            .getLogger(PingingClientCodecConnection.class);
    private final ScheduledExecutorService executor;
    private final AtomicReference<TimeValue> timeOut;
    private final AtomicLong nextTimeOut;
    private final AtomicReference<OpPing.Request> lastPing = new AtomicReference<OpPing.Request>(null);
    private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<ScheduledFuture<?>>(null);
    private final AtomicReference<PingingState> pingingState = new AtomicReference<PingingState>(PingingState.WAITING);

    private PingingClientCodecConnection(
            Publisher publisher,
            ClientProtocolCodec codec,
            Connection connection,
            ScheduledExecutorService executor,
            TimeValue timeOut) {
        super(publisher, codec, connection);
        this.executor = checkNotNull(executor);
        this.timeOut = new AtomicReference<TimeValue>(checkNotNull(timeOut));
        this.nextTimeOut = new AtomicLong(now() + timeOut.value(TIME_UNIT));
        if (codec.state() == ProtocolState.CONNECTED) {
            schedule();
        }
    }
    
    protected void schedule() {
        if (timeOut.get().value() != Session.Parameters.NEVER_TIMEOUT) {
            // somewhat arbitrary, but better than just a fixed interval...
            long tick = Math.max((nextTimeOut.get() - now()) / 2, 0);
            if (pingingState.compareAndSet(PingingState.WAITING, PingingState.SCHEDULED)) {
                future.set(executor.schedule(this, tick, TIME_UNIT));
            }
        }
    }
    
    protected void touch() {
        nextTimeOut.set(now() + timeOut.get().value(TIME_UNIT));
    }

    @Override
    public void run() {
        if (!pingingState.compareAndSet(PingingState.SCHEDULED, PingingState.WAITING)) {
            if (pingingState.get() == PingingState.STOPPED) {
                return;
            }
            // TODO: care if we were WAITING?
        }
        
        switch (asCodec().state()) {
        case ANONYMOUS:
        case CONNECTING:
            // try later
            schedule();
            return;
        case DISCONNECTING:
        case DISCONNECTED:
        case ERROR:
            stop();
            return;
        default:
            break;
        }

        // should ping now, or ok to wait a while?
        if (nextTimeOut.get() - now() > timeOut.get().value(TIME_UNIT) / 2) {
            schedule();
            return;
        }

        OpPing.Request ping = OpPing.Request.create();
        try {
            write(SessionRequestWrapper.create(ping.xid(), ping));
        } catch (Exception e) {
            stop();
            return;
        }
        
        lastPing.set(ping);
        if (logger.isTraceEnabled()) {
            logger.trace("PING: {}", ping);
        }
        schedule();
    }

    public void stop() {
        if (pingingState.getAndSet(PingingState.STOPPED) != PingingState.STOPPED) {
            ScheduledFuture<?> future = this.future.get();
            if (future != null && !(future.isDone())) {
                // run() shouldn't block, so no need to interrupt?
                future.cancel(false);
            }
        }
    }
    
    @Override
    protected void post(Object event) {
        if (event instanceof OpCreateSession.Response) {
            handleCreateSessionResponse((OpCreateSession.Response)event);
        } else if (event instanceof Operation.SessionReply) {
            Operation.SessionReply reply = (Operation.SessionReply)event;
            if (reply.reply() instanceof OpPing.Response) {
                handlePingResponse((OpPing.Response)reply.reply());
            }
        }
        super.post(event);
    }

    protected void handleCreateSessionResponse(OpCreateSession.Response message) {
        if (message instanceof OpCreateSession.Response.Valid) {
            timeOut.set(message.toParameters().timeOut());
            schedule();
        } else {
            stop();
        }
    }
    
    protected void handlePingResponse(OpPing.Response message) {
        if (logger.isTraceEnabled()) {
            // of course, this pong could be for an earlier ping,
            // so this time difference is not very accurate...
            OpPing.Request ping = lastPing.get();
            logger.trace(String.format("PONG %s: %s",
                    (ping == null) ? 0 : message.difference(ping), message));
        }
    }
    
    @Override
    public void write(Message.ClientSessionMessage message) throws IOException {
        touch();
        super.write(message);
    }
    
    @Subscribe
    @Override
    public void handleConnectionState(ConnectionStateEvent event) {
        Connection.State state = event.event().to();
        switch(state) {
        case CONNECTION_CLOSED:
            stop();
            break;
        default:
            break;
        }
        super.handleConnectionState(event);
    }

    @Subscribe
    public void handleProtocolState(AutomatonTransition<ProtocolState> event) {
        switch (event.to()) {
        case CONNECTED:
            schedule();
            break;
        case DISCONNECTING:
        case DISCONNECTED:
        case ERROR:
            stop();
            break;
        default:
            break;
        }
    }
}
