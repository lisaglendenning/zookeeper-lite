package edu.uw.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Logging;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Ping;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;

public class PingingClient<I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super Operation.Request>> extends ProtocolCodecConnection<I, T, C> {

    public static <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super Operation.Request>> ParameterizedFactory<Pair<Pair<Class<I>, T>, C>, PingingClient<I,T,C>> factory(
                final TimeValue defaultTimeOut,
                final ScheduledExecutorService executor) {
        return new ParameterizedFactory<Pair<Pair<Class<I>, T>, C>, PingingClient<I,T,C>>() {
                    @Override
                    public PingingClient<I,T,C> get(Pair<Pair<Class<I>, T>, C> value) {
                        return new PingingClient<I,T,C>(
                                TimeOutParameters.create(defaultTimeOut),
                                executor,
                                value.first().second(),
                                value.second());
                    }
                };
    }
    
    private final PingingTask pingTask;

    protected PingingClient(
            TimeOutParameters pingParameters,
            ScheduledExecutorService executor,
            T codec,
            C connection) {
        super(codec, connection);
        
        this.pingTask = new PingingTask(pingParameters, executor);
        if (codec.state() == ProtocolState.CONNECTED) {
            pingTask.run();
        }
    }

    @Override
    public <V extends I> ListenableFuture<V> write(V input) {
        pingTask.send(input);
        return super.write(input);
    }

    @Override
    @Subscribe
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if ((Connection.State.CONNECTION_CLOSING == event.to()) || 
                (Connection.State.CONNECTION_CLOSED == event.to()) ||
                (ProtocolState.DISCONNECTING == event.to()) || 
                (ProtocolState.DISCONNECTED == event.to()) || 
                (ProtocolState.ERROR == event.to())) {
            pingTask.stop();
        }
        
        super.handleTransitionEvent(event);
    }

    @Subscribe
    public void handleResponse(Message.Server message) {
        pingTask.send(message);
    }

    protected static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    
    protected class PingingTask implements Actor<Object> {

        private final TimeOutParameters pingParameters;
        private final ScheduledExecutorService executor;
        private final AtomicReference<State> state = new AtomicReference<State>(State.WAITING);        
        private volatile Ping.Request lastPing = null;
        private volatile ScheduledFuture<?> future = null;

        public PingingTask(
                TimeOutParameters pingParameters,
                ScheduledExecutorService executor) {
            this.pingParameters = checkNotNull(pingParameters);
            this.executor = checkNotNull(executor);
        }
        
        public Ping.Request lastPing() {
            return lastPing;
        }
        
        @Override
        public State state() {
            return state.get();
        }
        
        @Override
        public boolean send(Object message) {
            if (message instanceof Operation.Request) {
                pingParameters.touch();
            } else if (message instanceof Message.Server) {
                if (message instanceof ConnectMessage.Response) {
                    if (message instanceof ConnectMessage.Response.Valid) {
                        pingParameters.setTimeOut(((ConnectMessage.Response) message).toParameters().timeOut().value());
                        if (pingParameters.getTimeOut() != Session.Parameters.NEVER_TIMEOUT) {
                            pingParameters.touch();
                            run();
                        } else {
                            stop();
                        }
                    } else {
                        stop();
                    } 
                } else {
                    if (logger.isTraceEnabled()) {
                        if (message instanceof Message.ServerResponse) {
                            Records.Response response = ((Message.ServerResponse<?>) message).getRecord();
                            if (response instanceof Ping.Response) {
                                Ping.Response pong = (Ping.Response) response;
                                // of course, this pong could be for an earlier ping,
                                // so this time difference is not very accurate...
                                Ping.Request ping = pingTask.lastPing();
                                logger.trace(Logging.PING_MARKER, String.format("PONG %s: %s",
                                        (ping == null) ? 0 : pong.difference(ping), pong));
                            }
                        }
                    }
                }
            }
            return (state() != State.TERMINATED);
        }

        @Override
        public void run() {
            if (!state.compareAndSet(State.SCHEDULED, State.RUNNING) || schedule()) {
                return;
            }
            
            switch (codec().state()) {
            case ANONYMOUS:
            case CONNECTING:
                throw new AssertionError();
            case DISCONNECTING:
            case DISCONNECTED:
            case ERROR:
                stop();
                return;
            default:
                break;
            }
                
            // should ping now, or ok to wait a while?
            if (pingParameters.remaining() < pingParameters.getTimeOut() / 2) {
                Operation.ProtocolRequest<Ping.Request> message = ProtocolRequestMessage.from(Ping.Request.newInstance());
                try {
                    delegate().write(message);
                } catch (Exception e) {
                    stop();
                    return;
                }

                pingParameters.touch();
                lastPing = message.getRecord();
                if (logger.isTraceEnabled()) {
                    logger.trace(Logging.PING_MARKER, "PING: {}", lastPing);
                }
            }

            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                schedule();
            }
        }

        public synchronized boolean stop() {
            if (state.getAndSet(State.TERMINATED) != State.TERMINATED) {
                if ((future != null) && !future.isDone()) {
                    future.cancel(false);
                }
                return true;
            }
            return false;
        }
        
        private synchronized boolean schedule() {
            if (state.compareAndSet(State.WAITING, State.SCHEDULED)) {
                // somewhat arbitrary...
                long tick = Math.max(pingParameters.remaining() / 2, 0);
                future = executor.schedule(this, tick, TIME_UNIT);
                return true;
            }
            return false;
        }
    }
}
