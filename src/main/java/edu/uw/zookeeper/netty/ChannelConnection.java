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
import edu.uw.zookeeper.protocol.Decoder;
import edu.uw.zookeeper.protocol.Encoder;
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
    
    public static <I,O, C extends Connection<I>> ChannelConnectionFactory<I,O,C> factory(
            Factory<? extends Publisher> publisherFactory,
            Connection.CodecFactory<I,O,C> codecFactory) {
        return ChannelConnectionFactory.newInstance(publisherFactory, codecFactory);
    }

    public static class ChannelConnectionFactory<I,O, C extends Connection<I>> implements ParameterizedFactory<Channel, C> {

        public static <I,O, C extends Connection<I>> ChannelConnectionFactory<I,O,C> newInstance(
                Factory<? extends Publisher> publisherFactory,
                Connection.CodecFactory<I,O,C> codecFactory) {
            return new ChannelConnectionFactory<I,O,C>(publisherFactory, codecFactory);
        }
        
        private final Factory<? extends Publisher> publisherFactory;
        private final Connection.CodecFactory<I,O,C> codecFactory;
        
        private ChannelConnectionFactory(
                Factory<? extends Publisher> publisherFactory,
                Connection.CodecFactory<I,O,C> codecFactory) {
            super();
            this.publisherFactory = publisherFactory;
            this.codecFactory = codecFactory;
        }

        @Override
        public C get(Channel channel) {
            Publisher publisher = publisherFactory.get();
            ChannelConnection<I> channelConnection = ChannelConnection.newInstance(channel, publisher);
            Pair<C, ? extends Codec<? super I, Optional<? extends O>>> codec = codecFactory.get(channelConnection);
            attach(channel, codec.first(), codec.second(), codec.second());
            return codec.first();
        }
    }

    public class OutboundProcessor implements Processor<PromiseTask<I, I>, I> {
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

    public static <I,O> ChannelConnection<I> newInstance(
            Channel channel, 
            Publisher publisher, 
            Encoder<? super I> encoder, 
            Decoder<Optional<? extends O>> decoder) {
        ChannelConnection<I> connection = newInstance(channel, publisher);
        attach(connection.get(), connection, encoder, decoder);
        return connection;
    }
    
    protected static <I,O> Channel attach(
            Channel channel, 
            Publisher publisher,
            Encoder<? super I> encoder, 
            Decoder<Optional<? extends O>> decoder) {
        InboundHandler.attach(channel, publisher, decoder);
        OutboundHandler.attach(channel, encoder);
        ConnectionStateHandler.attach(channel, publisher);
        return channel;
    }

    protected static <I,O> ChannelConnection<I> newInstance(
            Channel channel, 
            Publisher publisher) {
        return new ChannelConnection<I>(channel, publisher);
    }
    
    protected final PublisherActor publisher;
    protected final Logger logger;
    protected final Channel channel;
    protected final TaskMailbox.ActorTaskExecutor<I,I> outbound;

    protected ChannelConnection(
            Channel channel,
            Publisher publisher) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.channel = checkNotNull(channel);
        this.publisher = PublisherActor.newInstance(publisher, this);
        this.outbound = TaskMailbox.executor(TaskMailbox.actor(new OutboundProcessor(), this));
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

    @Override
    public Channel get() {
        return channel;
    }

    @Override
    public State state() {
        ConnectionStateHandler stateHandler = get().pipeline().get(ConnectionStateHandler.class);
        return stateHandler.state();
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
        // Note that this may result in multiple events for the same buffer
        get().pipeline().fireInboundBufferUpdated();
    }

    @Override
    public ListenableFuture<I> write(I message) {
        return outbound.submit(message);
    }

    @Override
    public ListenableFuture<Connection<I>> flush() {
        ChannelFuture future = get().flush();
        ChannelFutureWrapper<Connection<I>> wrapper = ChannelFutureWrapper
                .of(future, (Connection<I>) this);
        return wrapper;
    }

    @Override
    public ListenableFuture<Connection<I>> close() {
        logger.debug("Closing: {}", this);
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
}
