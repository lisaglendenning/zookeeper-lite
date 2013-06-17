package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ClientSessionMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.OpPing;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.TimeValue;

public class PingingClientCodecConnection extends ClientCodecConnection implements Runnable {

    public static ParameterizedFactory<Pair<Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>, Connection<Message.ClientSessionMessage>>, PingingClientCodecConnection> factory(
            final TimeValue defaultTimeOut,
            final ScheduledExecutorService executor) {
        return new ParameterizedFactory<Pair<Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>, Connection<Message.ClientSessionMessage>>, PingingClientCodecConnection>() {
                    @Override
                    public PingingClientCodecConnection get(Pair<Pair<Class<Message.ClientSessionMessage>, ClientProtocolCodec>, Connection<Message.ClientSessionMessage>> value) {
                        return new PingingClientCodecConnection(
                                value.first().second(),
                                value.second(),
                                executor,
                                defaultTimeOut);
                    }
                };
    }

    private static enum PingingState {
        WAITING, SCHEDULED, STOPPED;
    }
    
    private static long now() {
        return System.currentTimeMillis();
    }
    
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final ScheduledExecutorService executor;
    private final AtomicReference<TimeValue> timeOut;
    private final AtomicLong nextTimeOut;
    private final AtomicReference<OpPing.Request> lastPing = new AtomicReference<OpPing.Request>(null);
    private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<ScheduledFuture<?>>(null);
    private final AtomicReference<PingingState> pingingState = new AtomicReference<PingingState>(PingingState.WAITING);

    protected PingingClientCodecConnection(
            ClientProtocolCodec codec,
            Connection<Message.ClientSessionMessage> connection,
            ScheduledExecutorService executor,
            TimeValue timeOut) {
        super(codec, connection);
        this.executor = checkNotNull(executor);
        this.timeOut = new AtomicReference<TimeValue>(checkNotNull(timeOut));
        this.nextTimeOut = new AtomicLong(now() + timeOut.value(TIME_UNIT));
        if (codec.state() == ProtocolState.CONNECTED) {
            schedule();
        }
    }
    
    protected void schedule() {
        if (timeOut.get().value() != Session.Parameters.NEVER_TIMEOUT) {
            if (pingingState.compareAndSet(PingingState.WAITING, PingingState.SCHEDULED)) {
                // somewhat arbitrary, but better than just a fixed interval...
                long tick = Math.max((nextTimeOut.get() - now()) / 2, 0);
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
        
        switch (codec().state()) {
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

        OpPing.Request ping = OpPing.Request.newInstance();
        try {
            write(SessionRequestMessage.newInstance(ping.xid(), ping));
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
    public ListenableFuture<ClientSessionMessage> write(Message.ClientSessionMessage message) {
        touch();
        return super.write(message);
    }

    @Subscribe
    public void handleCreateSessionResponse(ConnectMessage.Response message) {
        if (message instanceof ConnectMessage.Response.Valid) {
            timeOut.set(message.toParameters().timeOut());
            touch();
            schedule();
        } else {
            stop();
        }
    }

    @Subscribe
    public void handleSessionReply(Operation.SessionReply message) {
        if (message.reply() instanceof OpPing.Response) {
            handlePingResponse((OpPing.Response) message.reply());
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
    public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
        switch (event.to()) {
        case CONNECTION_CLOSED:
            stop();
            break;
        default:
            break;
        }

        super.handleConnectionStateEvent(event);
    }

    @Override
    public void handleProtocolStateEvent(Automaton.Transition<ProtocolState> event) {
        switch (event.to()) {
        case DISCONNECTED:
        case ERROR:
            stop();
            break;
        default:
            break;
        }
        
        super.handleProtocolStateEvent(event);
    }
}
