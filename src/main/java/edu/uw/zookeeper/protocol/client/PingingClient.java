package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
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
import edu.uw.zookeeper.protocol.TimeOutActor;
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

    protected class PingingTask extends TimeOutActor<Object> implements Actor<Object>, FutureCallback<Object> {

        private volatile Ping.Request lastPing = null;

        public PingingTask(
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            super(parameters, executor);
            lastPing = null;
        }
        
        public Ping.Request lastPing() {
            return lastPing;
        }
        
        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            close();
        }

        @Override
        public boolean send(Object message) {
            if (message instanceof Operation.Request) {
                parameters.touch();
            } else if (message instanceof Message.Server) {
                if (message instanceof ConnectMessage.Response) {
                    if (message instanceof ConnectMessage.Response.Valid) {
                        parameters.setTimeOut(((ConnectMessage.Response) message).toParameters().timeOut().value());
                        parameters.touch();
                        run();
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
        protected void doRun() {            
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
            if (parameters.remaining() < parameters.getTimeOut() / 2) {
                Operation.ProtocolRequest<Ping.Request> message = ProtocolRequestMessage.from(Ping.Request.newInstance());
                try {
                    Futures.addCallback(delegate().write(message), this);
                } catch (Exception e) {
                    close();
                    return;
                }

                parameters.touch();
                lastPing = message.getRecord();
                if (logger.isTraceEnabled()) {
                    logger.trace(Logging.PING_MARKER, "PING: {}", lastPing);
                }
            }
        }

        @Override
        protected synchronized void doSchedule() {
            if (parameters.getTimeOut() != Session.Parameters.NEVER_TIMEOUT) {
                // somewhat arbitrary...
                long tick = Math.max(parameters.remaining() / 2, 0);
                future = executor.schedule(this, tick, parameters.getUnit());
            } else {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            }
        }
    }
}
