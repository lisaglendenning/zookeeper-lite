package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actors.ExecutedQueuedActor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;

public abstract class AbstractChannelConnection<I,O,C extends AbstractChannelConnection<I,O,C>> 
        implements Connection<I,O,C> {

    protected final Logger logger;
    protected final EventfulHandler<? extends O> eventful;
    protected final ConnectionStateHandler state;
    protected final Channel channel;
    protected final OutboundActor outbound;

    protected AbstractChannelConnection(
            Channel channel,
            EventfulHandler<? extends O> eventful,
            ConnectionStateHandler state,
            Collection<? extends Connection.Listener<? super O>> listeners,
            Logger logger) {
        this.logger = checkNotNull(logger);
        this.eventful = checkNotNull(eventful);
        this.channel = checkNotNull(channel);
        this.state = checkNotNull(state);
        this.outbound = new OutboundActor();

        // subscribe any listeners before creating pipeline
        state.subscribe(eventful);
        for (Connection.Listener<? super O> listener: listeners) {
            eventful.subscribe(listener);
        }
        
        // finally, create pipeline
        ConnectionStateHandler.toPipeline(
                state, EventfulHandler.toPipeline(
                        eventful, channel.pipeline()));
    }
    
    @Override
    public void execute(Runnable runnable) {
        if (channel.isRegistered()) {
            channel.eventLoop().execute(runnable);
        } else {
            executeNow(runnable);
        }
    }
    
    protected synchronized void executeNow(Runnable runnable) {
        runnable.run();
    }

    public Channel channel() {
        return channel;
    }

    @Override
    public State state() {
        return state.state();
    }

    @Override
    public SocketAddress localAddress() {
        return channel.localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Override
    public C read() {
        channel.read();
        return self();
    }

    @Override
    public <T extends I> ListenableFuture<T> write(T message) {
        PromiseTask<T,T> task = PromiseTask.of(message, SettableFuturePromise.<T>create());
        if (! outbound.send(task)) {
            task.setException(new ClosedChannelException());
        }
        return task;
    }
    
    @Override
    public C flush() {
        channel.flush();
        return self();
    }

    @Override
    public ListenableFuture<? extends C> close() {
        return ChannelFutureWrapper.of(channel.close(), self());
    }

    @Override
    public void subscribe(Listener<? super O> listener) {
        eventful.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Listener<? super O> listener) {
        return eventful.unsubscribe(listener);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(channel).toString();
    }
    
    @SuppressWarnings("unchecked")
    protected C self() {
        return (C) this;
    }

    protected final class OutboundActor extends ExecutedQueuedActor<PromiseTask<? extends I, ? extends I>> implements ChannelFutureListener {

        private final ConcurrentLinkedQueue<PromiseTask<? extends I, ? extends I>> mailbox;
        
        public OutboundActor() {
            this.mailbox = Queues.newConcurrentLinkedQueue();
            channel.closeFuture().addListener(this);
        }

        @Override
        public void operationComplete(ChannelFuture future) {
            stop();
        }
        
        @Override
        public String toString() {
            return AbstractChannelConnection.this.toString();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        protected boolean apply(PromiseTask<? extends I, ? extends I> input) {
            if (! input.isDone()) {
                Connection.State state = AbstractChannelConnection.this.state();
                switch (state) {
                    case CONNECTION_CLOSING:
                    case CONNECTION_CLOSED:
                    {
                        input.setException(new ClosedChannelException());
                        break;
                    }
                    default:
                    {
                        I task = input.task();
                        ChannelFutureWrapper.of(
                                channel.write(task), task, (Promise<I>) input);
                        break;
                    }
                }
            }
            return true;
        }
        
        @Override
        protected void runExit() {
            channel.flush();
            super.runExit();
        }

        @Override
        protected void doStop() {
            Exception e = new ClosedChannelException();
            PromiseTask<? extends I, ? extends I> next;
            while ((next = mailbox.poll()) != null) {
                if (! next.isDone()) {
                    next.setException(e);
                }
            }
        }

        @Override
        protected ConcurrentLinkedQueue<PromiseTask<? extends I, ? extends I>> mailbox() {
            return mailbox;
        }

        @Override
        protected Executor executor() {
            return AbstractChannelConnection.this;
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }
}
