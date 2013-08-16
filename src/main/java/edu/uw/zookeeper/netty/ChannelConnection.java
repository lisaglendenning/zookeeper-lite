package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ActorPublisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;

public class ChannelConnection<I> 
        implements Connection<I>, Publisher, Executor {

    public static <I> ChannelConnection<I> newInstance(
            Channel channel,
            Publisher publisher) {
        ChannelConnection<I> instance = new ChannelConnection<I>(channel, publisher);
        instance.attach();
        return instance;
    }
    
    protected final Logger logger;
    protected final ActorPublisher publisher;
    protected final Automaton<Connection.State, Connection.State> state;
    protected final Channel channel;
    protected final OutboundActor outbound;

    protected ChannelConnection(
            Channel channel,
            Publisher publisher) {
        this.logger = LogManager.getLogger(getClass());
        this.channel = checkNotNull(channel);
        this.publisher = ActorPublisher.newInstance(
                LoggingPublisher.create(logger, publisher, this), 
                this);
        this.state = ConnectionStateHandler.newAutomaton(this);
        this.outbound = new OutboundActor();
    }
    
    protected void attach() {
        ConnectionStateHandler.attach(channel, state, logger);
        InboundHandler.attach(channel, this);
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

    protected Channel channel() {
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
    public void read() {
        channel.read();
    }

    @Override
    public <T extends I> ListenableFuture<T> write(T message) {
        PromiseTask<T,T> task = PromiseTask.of(message, SettableFuturePromise.<T>create());
        outbound.send(task);
        return task;
    }
    
    @Override
    public void flush() {
        channel.flush();
    }

    @Override
    public ListenableFuture<Connection<I>> close() {
        state.apply(State.CONNECTION_CLOSING);
        ChannelFuture future = channel.close();
        ChannelFutureWrapper<Connection<I>> wrapper = ChannelFutureWrapper
                .of(future, (Connection<I>) this);
        return wrapper;
    }

    @Override
    public void post(Object event) {
        publisher.post(event);
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(channel).toString();
    }

    protected class OutboundActor extends ExecutedActor<PromiseTask<? extends I, ? extends I>> {

        protected final ConcurrentLinkedQueue<PromiseTask<? extends I, ? extends I>> mailbox;
        protected volatile boolean doFlush;
        
        public OutboundActor() {
            this.mailbox = new ConcurrentLinkedQueue<PromiseTask<? extends I, ? extends I>>();
            this.doFlush = false;
        }
        
        @Override
        protected ConcurrentLinkedQueue<PromiseTask<? extends I, ? extends I>> mailbox() {
            return mailbox;
        }

        @Override
        protected Executor executor() {
            return ChannelConnection.this;
        }

        @Override
        protected void doRun() {
            try {
                super.doRun();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            
            if (doFlush) {
                doFlush = false;
                channel.flush();
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        protected boolean apply(PromiseTask<? extends I, ? extends I> input) {
            if (! input.isDone()) {
                if (state() != State.TERMINATED) {
                    Connection.State state = ChannelConnection.this.state();
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
                            ChannelFuture future = channel.write(task);
                            ChannelFutureWrapper.of(future, task, (Promise<I>) input);
                            doFlush = true;
                            break;
                        }
                    }
                } else {
                    input.cancel(true);
                }
            }
            return (state() != State.TERMINATED);
        }
        
        @Override
        protected void doStop() {
            PromiseTask<? extends I, ? extends I> next;
            while ((next = mailbox.poll()) != null) {
                if (! next.isDone()) {
                    next.cancel(true);
                }
            }
        }
    }
}
