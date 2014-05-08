package edu.uw.zookeeper.protocol.client;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actor;
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
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.PingTask;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.IPingRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class PingingClient<I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> extends ClientProtocolConnection<I,O,V,T> {

    public static <I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> ParameterizedFactory<T, PingingClient<I,O,V,T>> factory(
                final TimeValue defaultTimeOut,
                final ScheduledExecutorService executor) {
        return new ParameterizedFactory<T, PingingClient<I,O,V,T>>() {
                    @Override
                    public PingingClient<I,O,V,T> get(T value) {
                        return PingingClient.<I,O,V,T>newInstance(
                                TimeOutParameters.milliseconds(defaultTimeOut.value(TimeUnit.MILLISECONDS)),
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
    
    private final ClientPingTask<I,O,T> pingTask;

    protected PingingClient(
            TimeOutParameters pingParameters,
            ScheduledExecutorService executor,
            T connection) {
        super(connection);
        
        this.pingTask = ClientPingTask.<I,O,T>create(connection, pingParameters, executor);
        new PingTaskListener();
    }

    @Override
    public <I1 extends I> ListenableFuture<I1> write(I1 input) {
        pingTask.send(input);
        return super.write(input);
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
                        close();
                    }
                }
            }
        }
    }
    
    protected static final class ClientPingTask<I extends Operation.Request, O, T extends CodecConnection<? super I, ? extends O,? extends ProtocolCodec<?,?,?,?>,?>> extends PingTask<I,O,T> implements Connection.Listener<O>, Automatons.AutomatonListener<ProtocolState> {

        public static <I extends Operation.Request, O, T extends CodecConnection<? super I, ? extends O,? extends ProtocolCodec<?,?,?,?>,?>> ClientPingTask<I,O,T> create(
                T connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            Logger logger = LogManager.getLogger(ClientPingTask.class);
            ClientPingTask<I,O,T> task = new ClientPingTask<I,O,T>(
                    connection,
                    parameters, 
                    executor,
                    Sets.<Pair<Runnable,Executor>>newHashSet(),
                    LoggingPromise.create(logger, SettableFuturePromise.<Void>create()),
                    logger);
            // TODO: replay connection events?
            return task;
        }
        
        private long lastPing = 0L;

        @SuppressWarnings("unchecked")
        protected ClientPingTask(
                T connection,
                TimeOutParameters parameters,
                ScheduledExecutorService scheduler,
                Set<Pair<Runnable,Executor>> listeners,
                Promise<Void> promise,
                Logger logger) {
            super((I) ProtocolRequestMessage.from(Records.newInstance(IPingRequest.class)), connection, parameters, scheduler, listeners, promise, logger);
            
            connection.codec().subscribe(this);
        }
        
        public synchronized long lastPing() {
            return lastPing;
        }
        
        @Override
        public void handleConnectionRead(O message) {
            if (message instanceof ConnectMessage.Response) {
                if (message instanceof ConnectMessage.Response.Valid) {
                    synchronized (this) {
                        parameters.setTimeOut(((ConnectMessage.Response) message).getTimeOut());
                        parameters.setTouch();
                        schedule();
                    }
                } else {
                    stop();
                }
            } else {
                if (logger.isTraceEnabled()) {
                    if (message instanceof Operation.ProtocolResponse<?>) {
                        if (((Operation.ProtocolResponse<?>) message).record().opcode() == OpCode.PING) {
                            synchronized (this) {
                                // of course, this pong could be for an earlier ping,
                                // so this time difference is not very accurate...
                                long pong = parameters.getNow();
                                assert (pong > lastPing);
                                logger.trace(
                                        LoggingMarker.PING_MARKER.get(), String.format("PONG %s: %s",
                                        (lastPing > 0L) ? (pong - lastPing) : "null", this));
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            switch (transition.to()) {
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

        @Override
        protected synchronized void doSchedule() {
            T connection = this.connection.get();
            if ((connection == null) || (connection.codec().state() != ProtocolState.CONNECTED)) {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            } else {
                super.doSchedule();
            }
        }

        @Override
        protected synchronized void ping() {
            T connection = this.connection.get();
            if (connection == null) {
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
            
            super.ping();
            
            if (state() != Actor.State.TERMINATED) {
                lastPing = parameters.getTouch();
            }
        }
        
        @Override
        protected synchronized void doStop() {
            T connection = this.connection.get();
            if (connection != null) {
                connection.codec().unsubscribe(this);
            }
            
            super.doStop();
        }
        
        @Override   
        protected synchronized Objects.ToStringHelper toStringHelper() {
            return super.toStringHelper().add("lastPing", lastPing);
        }
    }
}
