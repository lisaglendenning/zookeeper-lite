package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;

import org.apache.logging.log4j.Logger;

import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.LoggingMarker;
import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;

public class EventfulHandler<O> extends SimpleChannelInboundHandler<O> implements Eventful<Connection.Listener<? super O>>, Automatons.AutomatonListener<Connection.State>, Connection.Listener<O> {

    public static EventfulHandler<?> fromPipeline(ChannelPipeline pipeline) {
        return (EventfulHandler<?>) pipeline.get(EventfulHandler.class.getName());
    }

    public static ChannelPipeline toPipeline(
            EventfulHandler<?> handler,
            ChannelPipeline pipeline) {
        return pipeline.addLast(
                EventfulHandler.class.getName(), handler);
    }

    public static <O> EventfulHandler<O> withLogger(
            Class<? extends O> type,
            Logger logger) {
        return newInstance(type, EventfulHandler.<O>newListeners(), logger);
    }
    
    public static <O> EventfulHandler<O> newInstance(
            Class<? extends O> type,
            IConcurrentSet<Connection.Listener<? super O>> listeners,
            Logger logger) {
        return new EventfulHandler<O>(type, listeners, logger);
    }
    
    public static <O> IConcurrentSet<Connection.Listener<? super O>> newListeners() {
        return new StrongConcurrentSet<Connection.Listener<? super O>>();
    }

    private final Logger logger;
    private final IConcurrentSet<Connection.Listener<? super O>> listeners;

    protected EventfulHandler(
            Class<? extends O> type,
            IConcurrentSet<Connection.Listener<? super O>> listeners,
            Logger logger) {
        super(type, true);
        this.logger = checkNotNull(logger);
        this.listeners = checkNotNull(listeners);
    }

    @Override
    public void subscribe(Connection.Listener<? super O> listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(Connection.Listener<? super O> listener) {
        return listeners.remove(listener);
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<Connection.State> transition) {
        handleConnectionState(transition);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        Iterator<?> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            itr.next();
        }
        
        super.handlerRemoved(ctx);
    }
    
    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
        for (Connection.Listener<? super O> listener: listeners) {
            listener.handleConnectionState(state);
        }
    }

    @Override
    public void handleConnectionRead(O message) {
        for (Connection.Listener<? super O> listener: listeners) {
            listener.handleConnectionRead(message);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, O message)  {
        if (logger.isTraceEnabled()) {
            logger.trace(LoggingMarker.NET_MARKER.get(), "READ {} ({})", message, ctx.channel());
        }
        handleConnectionRead(message);
    }
}
