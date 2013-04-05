package org.apache.zookeeper.protocol.client;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.data.OpPingAction;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.event.ConnectionEventValue;
import org.apache.zookeeper.event.ConnectionStateEvent;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.ConfigurableTime;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

public class PingSessionsTask implements Configurable {

    public static final String PARAM_KEY_PING_TICK = "Sessions.PingTick";
    public static final long PARAM_DEFAULT_PING_TICK = 1000;
    public static final String PARAM_KEY_PING_TICK_UNIT = "Sessions.PingTickUnit";
    public static final String PARAM_DEFAULT_PING_TICK_UNIT = "MILLISECONDS";
    
    public class PingConnectionTask implements Runnable {
        
        protected final Logger logger = LoggerFactory.getLogger(PingConnectionTask.class);
        protected final Connection connection;
        protected OpPingAction.Request lastPing = null;
        protected ScheduledFuture<?> future = null;
        
        @Inject
        public PingConnectionTask(Connection connection) {
            this.connection = checkNotNull(connection);
            connection.register(this);
        }

        public void start() {
            if (future == null) {
                long tick = tickTime.time();
                TimeUnit tickUnit = tickTime.timeUnit();
                future = executor.scheduleAtFixedRate(this, tick, tick, tickUnit);
            }
        }
        
        @Override
        public void run() {
            lastPing = OpPingAction.Request.create();
            if (logger.isTraceEnabled()) {
                logger.trace("PING: {}", lastPing);
            }
            switch (connection.state()) {
            case CONNECTION_OPENED:
                connection.send(lastPing);
                break;
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                stop();
                break;
            default:
                break;
            }
        }

        public void stop() {
            if (future != null) {
                connection.unregister(this);
                future.cancel(true);
                future = null;
            }
        }

        @Subscribe
        public void handleConnectionStateEvent(ConnectionStateEvent event) {
            switch(event.event()) {
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                stop();
                break;
            default:
                break;
            }
        }
        
        @Subscribe
        public void handleConnectionEvent(ConnectionEventValue<?> event) {
            if (event.event() instanceof Operation.Response) {
                handleOperationResponseEvent((Operation.Response) event.event());
            } else if (event.event() instanceof SessionConnection.State) {
                handleSessionConnectionStateEvent((SessionConnection.State) event.event());
            }
        }

        @Subscribe
        public void handleOperationResponseEvent(Operation.Response event) {
            if (event.operation() == Operation.PING) {
                while (true) {
                    if (event instanceof OpPingAction.Response) {
                        handlePingResponse((OpPingAction.Response)event);
                        break;
                    } else if (event instanceof Operation.ResponseValue
                            && ((Operation.ResponseValue)event).response() instanceof Operation.Response) {
                        event = ((Operation.ResponseValue)event).response();
                    } else {
                        break;
                    }
                }
            }            
        }
        
        @Subscribe
        public void handlePingResponse(OpPingAction.Response response) {
            if (logger.isTraceEnabled()) {
                logger.trace(String.format(
                        "PONG %d %s: %s",
                        response.difference(lastPing),
                        response.timeUnit().name(),
                        response));
            }            
        }
        
        @Subscribe
        public void handleSessionConnectionStateEvent(SessionConnection.State event) {
            switch (event) {
            case CONNECTED:
                start();
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

    protected final Logger logger = LoggerFactory.getLogger(PingSessionsTask.class);
    protected final ScheduledExecutorService executor;
    protected final ConfigurableTime tickTime;
    
    @Inject
    public PingSessionsTask(
            Configuration configuration,
            ClientConnectionGroup connections,
            ScheduledExecutorService executor) {
        super();
        this.executor = executor;
        this.tickTime = ConfigurableTime.create(
                PARAM_KEY_PING_TICK, 
                PARAM_DEFAULT_PING_TICK, 
                PARAM_KEY_PING_TICK_UNIT, 
                PARAM_DEFAULT_PING_TICK_UNIT);
        configure(configuration);
        connections.register(this);
    }

    @Override
    public void configure(Configuration configuration) {
        tickTime.configure(configuration);
    }
    
    @Subscribe
    public void handleConnection(Connection connection) {
        newTask(connection);
    }

    protected PingConnectionTask newTask(Connection connection) {
        PingConnectionTask task = new PingConnectionTask(connection);
        //task.start();
        return task;
    }
}