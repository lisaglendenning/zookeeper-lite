package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.SocketAddress;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Codec;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PublisherActor;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TaskMailbox;

public class ChannelConnection<I> 
        implements Connection<I>, Publisher, Reference<Channel>, Executor {
    
    public static <I, O, T extends Codec<I,Optional<O>>, C extends Connection<I>> FromCodecFactory<I,O,T,C> factory(
            Factory<? extends Publisher> publisherFactory,
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        return FromCodecFactory.newInstance(publisherFactory, codecFactory, connectionFactory);
    }

    public static class FromCodecFactory<I, O, T extends Codec<I,Optional<O>>, C extends Connection<I>> implements ParameterizedFactory<Channel, C> {

        public static <I, O, T extends Codec<I,Optional<O>>, C extends Connection<I>> FromCodecFactory<I,O,T,C> newInstance(
                Factory<? extends Publisher> publisherFactory,
                ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
                ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
            return new FromCodecFactory<I,O,T,C>(publisherFactory, codecFactory, connectionFactory);
        }
        
        private final Factory<? extends Publisher> publisherFactory;
        private final ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory;
        private final ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory;
        
        private FromCodecFactory(
                Factory<? extends Publisher> publisherFactory,
                ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
                ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
            super();
            this.publisherFactory = publisherFactory;
            this.codecFactory = codecFactory;
            this.connectionFactory = connectionFactory;
        }

        @Override
        public C get(Channel channel) {
            Publisher publisher = publisherFactory.get();
            ChannelConnection<I> connection = new ChannelConnection<I>(channel, publisher);
            Pair<Class<I>,T> codec = codecFactory.get(connection);
            connection.attach(codec.first(), codec.second());
            return connectionFactory.get(Pair.<Pair<Class<I>, T>, Connection<I>>create(codec, connection));
        }
    }

    protected final PublisherActor publisher;
    protected final Automaton<Connection.State, Connection.State> state;
    protected final Logger logger;
    protected final Channel channel;
    protected final TaskMailbox.ActorTaskExecutor<I,I> outbound;

    protected ChannelConnection(
            Channel channel,
            Publisher publisher) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.channel = checkNotNull(channel);
        this.publisher = PublisherActor.newInstance(publisher, this);
        this.state = ConnectionStateHandler.newAutomaton(this);
        this.outbound = TaskMailbox.executor(TaskMailbox.actor(new OutboundProcessor(), this));
    }
    
    protected <O> void attach(
            Class<I> inputType,
            Codec<I, Optional<O>> codec) {
        OutboundHandler.attach(get(), inputType, codec);
        ConnectionStateHandler.attach(get(), state);
        DecoderHandler.attach(get(), codec);
        InboundHandler.attach(get(), this);
    }
    
    @Override
    public void execute(Runnable runnable) {
        if (get().isRegistered()) {
            get().eventLoop().execute(runnable);
        } else {
            executeNow(runnable);
        }
    }
    
    protected synchronized void executeNow(Runnable runnable) {
        runnable.run();
    }

    @Override
    public Channel get() {
        return channel;
    }

    @Override
    public State state() {
        return this.state.state();
    }

    @Override
    public SocketAddress localAddress() {
        return get().localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
        return get().remoteAddress();
    }

    @Override
    public void read() {
        get().read();
    }

    @Override
    public ListenableFuture<I> write(I message) {
        return outbound.submit(message);
    }

    @Override
    public ListenableFuture<Connection<I>> close() {
        if(state.apply(State.CONNECTION_CLOSING).orNull() == State.CONNECTION_CLOSING) {
            logger.debug("Closing: {}", this);
        }
        ChannelFuture future = get().close();
        ChannelFutureWrapper<Connection<I>> wrapper = ChannelFutureWrapper
                .of(future, (Connection<I>) this);
        return wrapper;
    }

    @Override
    public void post(Object event) {
        logger.trace("{} ({})", event, this);
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
                .add("channel", get()).toString();
    }

    protected class OutboundProcessor implements Processor<PromiseTask<I, I>, I> {
        @Override
        public I apply(PromiseTask<I, I> input) throws Exception {
            Connection.State state = state();
            switch (state) {
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                input.setException(new IllegalStateException(state.toString()));
                break;
            default:
                break;
            }
            I task = input.task();
            if (! input.isDone()) {
                ChannelFuture future = get().write(task);
                ChannelFutureWrapper.of(future, task, input);
            }
            return task;
        }
    }
}
