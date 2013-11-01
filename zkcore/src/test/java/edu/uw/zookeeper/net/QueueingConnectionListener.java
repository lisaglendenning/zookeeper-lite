package edu.uw.zookeeper.net;

import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.common.Automaton;

public class QueueingConnectionListener<O> implements Connection.Listener<O> {

    public static <O> QueueingConnectionListener<O> linkedQueues() {
        return new QueueingConnectionListener<O>(
                Queues.<Automaton.Transition<Connection.State>>newLinkedBlockingQueue(),
                Queues.<O>newLinkedBlockingQueue(),
                LogManager.getLogger(QueueingConnectionListener.class));
    }
    
    public class Unsubscriber implements Connection.Listener<O> {
        
        protected final Connection<?,? extends O,?> connection;
        
        public Unsubscriber(Connection<?,? extends O,?> connection) {
            this.connection = connection;
            connection.subscribe(QueueingConnectionListener.this);
            connection.subscribe(this);
            if (connection.state() == Connection.State.CONNECTION_CLOSED) {
                handleConnectionState(Automaton.Transition.create(connection.state(), connection.state()));
            }
        }

        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> state) {
            if (state.to() == Connection.State.CONNECTION_CLOSED) {
                connection.unsubscribe(this);
                connection.unsubscribe(QueueingConnectionListener.this);
            }
        }

        @Override
        public void handleConnectionRead(O message) {
        }
    }
    
    protected final BlockingQueue<Automaton.Transition<Connection.State>> states;
    protected final BlockingQueue<O> reads;
    protected final Logger logger;
    
    public QueueingConnectionListener(
            BlockingQueue<Automaton.Transition<Connection.State>> states,
            BlockingQueue<O> reads,
            Logger logger) {
        this.states = states;
        this.reads = reads;
        this.logger = logger;
    }
    
    public Unsubscriber subscribe(Connection<?,? extends O,?> connection) {
        return new Unsubscriber(connection);
    }

    public BlockingQueue<Automaton.Transition<Connection.State>> states() {
        return states;
    }
    
    public BlockingQueue<O> reads() {
        return reads;
    }

    @Override
    public void handleConnectionState(
            Automaton.Transition<Connection.State> state) {
        logger.trace("STATE {} ({})", state, this);
        try {
            states.put(state);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void handleConnectionRead(O message) {
        logger.trace("READ {} ({})", message, this);
        try {
            reads.put(message);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}
