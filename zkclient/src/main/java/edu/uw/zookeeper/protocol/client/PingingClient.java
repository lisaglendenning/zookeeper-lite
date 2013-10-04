package edu.uw.zookeeper.protocol.client;

import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class PingingClient<I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> extends ProtocolCodecConnection<I, T, C> {

    public static <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super I>> ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, PingingClient<I,T,C>> factory(
                final TimeValue defaultTimeOut,
                final ScheduledExecutorService executor) {
        return new ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, C>, PingingClient<I,T,C>>() {
                    @Override
                    public PingingClient<I,T,C> get(Pair<? extends Pair<Class<I>, ? extends T>, C> value) {
                        return new PingingClient<I,T,C>(
                                TimeOutParameters.create(defaultTimeOut),
                                executor,
                                value.first().second(),
                                value.second());
                    }
                };
    }
    
    private final PingingTask<I> pingTask;

    protected PingingClient(
            TimeOutParameters pingParameters,
            ScheduledExecutorService executor,
            T codec,
            C connection) {
        super(codec, connection);
        
        this.pingTask = new PingingTask<I>(pingParameters, executor, this);
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

    protected static class PingingTask<I extends Operation.Request> extends TimeOutActor<Object> implements Actor<Object>, FutureCallback<Object> {

        private final Logger logger = LogManager.getLogger(getClass());
        private final Message.ClientRequest<IPingRequest> pingRequest;
        private volatile TimeValue lastPing = null;
        private final WeakReference<PingingClient<I,?,?>> connection;

        public PingingTask(
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                PingingClient<I,?,?> connection) {
            super(parameters, executor);
            this.connection = new WeakReference<PingingClient<I,?,?>>(connection);
            this.pingRequest = ProtocolRequestMessage.from(Records.newInstance(IPingRequest.class));
            this.lastPing = null;
        }
        
        public TimeValue lastPing() {
            return lastPing;
        }
        
        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            PingingClient<I,?,?> connection = this.connection.get();
            if (connection != null) {
                connection.close();
            }
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
                            Records.Response response = ((Message.ServerResponse<?>) message).record();
                            if (response.opcode() == OpCode.PING) {
                                TimeValue pong = TimeValue.milliseconds(System.currentTimeMillis());
                                // of course, this pong could be for an earlier ping,
                                // so this time difference is not very accurate...
                                logger.trace(Logging.PING_MARKER, String.format("PONG %s: %s",
                                        (lastPing == null) ? 0 : pong.difference(lastPing), pong));
                            }
                        }
                    }
                }
            }
            return (state() != State.TERMINATED);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doRun() {   
            PingingClient<I,?,?> connection = this.connection.get();
            if (connection == null) {
                return;
            }
            
            switch (connection.codec().state()) {
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
                try {
                    Futures.addCallback(connection.write((I) pingRequest), this);
                } catch (Exception e) {
                    connection.close();
                    return;
                }

                lastPing = TimeValue.milliseconds(System.currentTimeMillis());
                if (logger.isTraceEnabled()) {
                    logger.trace(Logging.PING_MARKER, "PING: {}", lastPing);
                }
            }
        }

        @Override
        protected synchronized void doSchedule() {
            if (parameters.getTimeOut() != Session.Parameters.NEVER_TIMEOUT) {
                if (executor.isShutdown()) {
                    stop();
                } else {
                    // somewhat arbitrary...
                    long tick = Math.max(parameters.remaining() / 2, 0);
                    future = executor.schedule(this, tick, parameters.getUnit());
                }
            } else {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            }
        }
    }
}
