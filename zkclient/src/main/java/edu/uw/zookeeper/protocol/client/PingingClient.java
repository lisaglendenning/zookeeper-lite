package edu.uw.zookeeper.protocol.client;

import java.lang.ref.WeakReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.LoggingMarker;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class PingingClient<I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> extends ClientProtocolConnection<I,O,V,T> implements FutureCallback<Object> {

    public static <I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> ParameterizedFactory<T, PingingClient<I,O,V,T>> factory(
                final TimeValue defaultTimeOut,
                final ScheduledExecutorService executor) {
        return new ParameterizedFactory<T, PingingClient<I,O,V,T>>() {
                    @Override
                    public PingingClient<I,O,V,T> get(T value) {
                        return PingingClient.<I,O,V,T>newInstance(
                                TimeOutParameters.create(defaultTimeOut),
                                executor,
                                value);
                    }
                };
    }

    public static <I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> PingingClient<I,O,V,T> newInstance(
            TimeOutParameters pingParameters,
            ScheduledExecutorService executor,
            T connection) {
        return new PingingClient<I,O,V,T>(pingParameters, executor, connection);
    }
    
    private final PingingTask<I,O,T> pingTask;

    protected PingingClient(
            TimeOutParameters pingParameters,
            ScheduledExecutorService executor,
            T connection) {
        super(connection);
        
        this.pingTask = PingingTask.<I,O,T>create(connection, pingParameters, executor);
        new PingTaskListener();
        if (codec().state() == ProtocolState.CONNECTED) {
            pingTask.run();
        }
    }

    @Override
    public <I1 extends I> ListenableFuture<I1> write(I1 input) {
        pingTask.send(input);
        return super.write(input);
    }

    @Override
    public void onSuccess(Object result) {
    }

    @Override
    public void onFailure(Throwable t) {
        close();
    }

    protected class PingTaskListener implements Runnable {

        public PingTaskListener() {
            pingTask.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void run() {
            if (pingTask.isDone()) {
                if (! pingTask.isCancelled()) {
                    try {
                        pingTask.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        onFailure(e.getCause());
                    }
                }
            }
        }
    }
    
    protected static class PingingTask<I extends Operation.Request, O, T extends CodecConnection<? super I, ? extends O,? extends ProtocolCodec<?,?,?,?>,?>> extends TimeOutActor<I,Void> implements Connection.Listener<O>, Automatons.AutomatonListener<ProtocolState>, FutureCallback<I> {

        public static <I extends Operation.Request, O, T extends CodecConnection<? super I, ? extends O,? extends ProtocolCodec<?,?,?,?>,?>> PingingTask<I,O,T> create(
                T connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            Logger logger = LogManager.getLogger(PingingTask.class);
            return new PingingTask<I,O,T>(
                    connection,
                    parameters, 
                    executor,
                    new WeakConcurrentSet<Pair<Runnable,Executor>>(),
                    LoggingPromise.create(logger, SettableFuturePromise.<Void>create()),
                    logger);
        }
        
        private final Message.ClientRequest<IPingRequest> pingRequest;
        private final WeakReference<T> connection;
        private volatile TimeValue lastPing = null;

        public PingingTask(
                T connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                IConcurrentSet<Pair<Runnable,Executor>> listeners,
                Promise<Void> promise,
                Logger logger) {
            super(parameters, executor, listeners, promise, logger);
            this.connection = new WeakReference<T>(connection);
            this.pingRequest = ProtocolRequestMessage.from(
                    Records.newInstance(IPingRequest.class));
            this.lastPing = null;
            
            connection.subscribe(this);
            connection.codec().subscribe(this);
        }
        
        public TimeValue lastPing() {
            return lastPing;
        }
        
        @Override
        public void handleConnectionRead(O message) {
            if (logger.isTraceEnabled()) {
                if (message instanceof Operation.ProtocolResponse<?>) {
                    if (((Operation.ProtocolResponse<?>) message).record().opcode() == OpCode.PING) {
                        TimeValue pong = TimeValue.milliseconds(System.currentTimeMillis());
                        // of course, this pong could be for an earlier ping,
                        // so this time difference is not very accurate...
                        logger.trace(LoggingMarker.PING_MARKER.get(), String.format("PONG %s: %s",
                                (lastPing == null) ? 0 : pong.difference(lastPing), pong));
                    }
                }
            }
        }

        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> state) {
            switch (state.to()) {
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                stop();
                break;
            default:
                break;
            }
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            switch (transition.to()) {
            case CONNECTED:
                run();
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

        @Override
        public void onSuccess(I result) {
        }

        @Override
        public void onFailure(Throwable t) {
            promise.setException(t);
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("connection", connection.get()).toString();
        }

        @Override
        protected synchronized void doSchedule() {
            if (parameters.getTimeOut() != NEVER_TIMEOUT) {
                if ((connection.get() != null) && !isDone() && !scheduler.isShutdown()) {
                    // somewhat arbitrary...
                    long tick = Math.max(parameters.remaining() / 2, 0);
                    scheduled = scheduler.schedule(this, tick, parameters.getUnit());
                } else {
                    stop();
                }
            } else {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            }
        }

        @Override
        protected boolean doSend(I message) {
            parameters.touch();
            if (message instanceof ConnectMessage.Response) {
                if (message instanceof ConnectMessage.Response.Valid) {
                    parameters.setTimeOut(((ConnectMessage.Response) message).toParameters().timeOut().value());
                    run();
                } else {
                    stop();
                }
            }
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doRun() {
            T connection = this.connection.get();
            if (isDone() || (connection == null) || (connection.state() == Connection.State.CONNECTION_CLOSED)) {
                stop();
                return;
            }
            
            switch (connection.codec().state()) {
            case ANONYMOUS:
            case CONNECTING:
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
            if (parameters.remaining() < parameters.getTimeOut() / 2) {
                parameters.touch();
                lastPing = TimeValue.milliseconds(System.currentTimeMillis());
                logger.trace(LoggingMarker.PING_MARKER.get(), "PING: {}", lastPing);
                Futures.addCallback(connection.write((I) pingRequest), this, SameThreadExecutor.getInstance());
            }
        }

        @Override
        protected synchronized void doStop() {
            T connection = this.connection.get();
            if (connection != null) {
                connection.unsubscribe(this);
                connection.codec().unsubscribe(this);
            }
            
            super.doStop();
        }
    }
}
