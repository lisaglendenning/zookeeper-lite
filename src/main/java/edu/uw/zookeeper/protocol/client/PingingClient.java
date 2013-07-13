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
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Ping;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TimeValue;

public class PingingClient<I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super Operation.Request>> extends ProtocolCodecConnection<I, T, C> {

    public static <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super Operation.Request>> ParameterizedFactory<Pair<Pair<Class<I>, T>, C>, PingingClient<I,T,C>> factory(
                final TimeValue defaultTimeOut,
                final ScheduledExecutorService executor) {
        return new ParameterizedFactory<Pair<Pair<Class<I>, T>, C>, PingingClient<I,T,C>>() {
                    @Override
                    public PingingClient<I,T,C> get(Pair<Pair<Class<I>, T>, C> value) {
                        return new PingingClient<I,T,C>(
                                PingingParameters.newInstance(defaultTimeOut),
                                executor,
                                value.first().second(),
                                value.second());
                    }
                };
    }
    
    protected static long now() {
        return System.currentTimeMillis();
    }

    protected static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    public static class PingingParameters {

        public static PingingParameters newInstance(TimeValue timeOut) {
            return new PingingParameters(timeOut, now());
        }
        
        private final AtomicReference<TimeValue> timeOut;
        private final AtomicLong nextTimeOut;
        
        public PingingParameters(TimeValue timeOut, long now) {
            this.timeOut = new AtomicReference<TimeValue>(checkNotNull(timeOut));
            this.nextTimeOut = new AtomicLong(now + timeOut.value(TIME_UNIT));
        }
        
        public AtomicReference<TimeValue> timeOut() {
            return timeOut;
        }
        
        public AtomicLong nextTimeOut() {
            return nextTimeOut;
        }

        public void touch() {
            nextTimeOut.set(now() + timeOut.get().value(TIME_UNIT));
        }
    }

    private final ScheduledExecutorService executor;
    private final PingingParameters pingParameters;
    private final PingingTask pingTask;

    protected PingingClient(
            PingingParameters pingParameters,
            ScheduledExecutorService executor,
            T codec,
            C connection) {
        super(codec, connection);
        
        this.executor = checkNotNull(executor);
        this.pingParameters = checkNotNull(pingParameters);
        this.pingTask = new PingingTask();
        
        if (codec.state() == ProtocolState.CONNECTED) {
            pingTask.schedule();
        }
    }
    
    public PingingParameters pingParameters() {
        return pingParameters;
    }
    
    public PingingTask pingTask() {
        return pingTask;
    }

    @Override
    public <V extends I> ListenableFuture<V> write(V input) {
        pingParameters.touch();
        return super.write(input);
    }

    @Override
    public ListenableFuture<Connection<I>> close() {
        pingTask.stop();
        return super.close();
    }

    @Subscribe
    public void handleCreateSessionResponse(ConnectMessage.Response message) {
        if (message instanceof ConnectMessage.Response.Valid) {
            pingParameters.timeOut().set(message.toParameters().timeOut());
            pingParameters.touch();
            pingTask.schedule();
        } else {
            pingTask.stop();
        }
    }

    @Subscribe
    public void handleSessionReply(Message.ServerResponse<?> message) {
        if (logger.isTraceEnabled()) {
            if (message.getRecord() instanceof Ping.Response) {
                Ping.Response pong = (Ping.Response) message.getRecord();
                // of course, this pong could be for an earlier ping,
                // so this time difference is not very accurate...
                Ping.Request ping = pingTask.lastPing();
                logger.trace(String.format("PONG %s: %s",
                        (ping == null) ? 0 : pong.difference(ping), pong));
            }
        }
    }

    public static enum PingingState {
        WAITING, SCHEDULED, STOPPED;
    }
    
    protected class PingingTask implements Runnable, Stateful<PingingState> {

        private final AtomicReference<Ping.Request> lastPing = new AtomicReference<Ping.Request>(null);
        private final AtomicReference<ScheduledFuture<?>> future = new AtomicReference<ScheduledFuture<?>>(null);
        private final AtomicReference<PingingState> pingingState = new AtomicReference<PingingState>(PingingState.WAITING);

        public Ping.Request lastPing() {
            return lastPing.get();
        }
        
        @Override
        public PingingState state() {
            return pingingState.get();
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
            if (pingParameters().nextTimeOut().get() - now() > pingParameters().timeOut().get().value(TIME_UNIT) / 2) {
                schedule();
                return;
            }

            Ping.Request ping = Ping.Request.newInstance();
            Operation.ProtocolRequest<Ping.Request> message = ProtocolRequestMessage.from(ping);
            try {
                delegate().write(message);
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

        public void schedule() {
            if (pingParameters().timeOut().get().value() != Session.Parameters.NEVER_TIMEOUT) {
                if (pingingState.compareAndSet(PingingState.WAITING, PingingState.SCHEDULED)) {
                    // somewhat arbitrary, but better than just a fixed interval...
                    long tick = Math.max((pingParameters().nextTimeOut().get() - now()) / 2, 0);
                    future.set(executor.schedule(this, tick, TIME_UNIT));
                }
            }
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
    }
}
