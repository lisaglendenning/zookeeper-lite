package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Automatons.AutomatonListener;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.LoggingMarker;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

public class ConnectionStateHandler extends ChannelDuplexHandler implements Stateful<Connection.State>, Eventful<Automatons.AutomatonListener<Connection.State>> {

    public static Connection.State getChannelState(Channel channel) {
        Connection.State state = channel.isActive() ? Connection.State.CONNECTION_OPENED
                : (channel.isOpen() ? Connection.State.CONNECTION_OPENING
                        : Connection.State.CONNECTION_CLOSED);
        return state;
    }
    
    public static ConnectionStateHandler fromPipeline(ChannelPipeline pipeline) {
        return (ConnectionStateHandler) pipeline.get(ConnectionStateHandler.class.getName());
    }

    public static ChannelPipeline toPipeline(
            ConnectionStateHandler handler,
            ChannelPipeline pipeline) {
        return pipeline.addLast(
                ConnectionStateHandler.class.getName(), handler);
    }

    public static ConnectionStateHandler withLogger(
            Logger logger) {
        return new ConnectionStateHandler(newAutomaton(), logger);
    }
    
    public static ConnectionStateHandler newInstance(
            Automatons.EventfulAutomaton<Connection.State, Connection.State> state,
            Logger logger) {
        return new ConnectionStateHandler(state, logger);
    }
    
    public static Automatons.EventfulAutomaton<Connection.State, Connection.State> newAutomaton() {
        return Automatons.createSynchronizedEventful(
                Automatons.createEventful(
                        Automatons.createSimple(
                                Connection.State.CONNECTION_OPENING)));
    }

    private final Logger logger;
    private final Automatons.EventfulAutomaton<Connection.State, Connection.State> automaton;

    protected ConnectionStateHandler(
            Automatons.EventfulAutomaton<Connection.State, Connection.State> automaton,
            Logger logger) {
        this.logger = checkNotNull(logger);
        this.automaton = checkNotNull(automaton);
    }

    @Override
    public Connection.State state() {
        return automaton.state();
    }

    @Override
    public void subscribe(AutomatonListener<Connection.State> listener) {
        automaton.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(AutomatonListener<Connection.State> listener) {
        return automaton.unsubscribe(listener);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        apply(ctx, getChannelState(ctx.channel()));
        super.handlerAdded(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        apply(ctx, Connection.State.CONNECTION_OPENING);
        apply(ctx, Connection.State.CONNECTION_OPENED);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        apply(ctx, Connection.State.CONNECTION_CLOSING);
        apply(ctx, Connection.State.CONNECTION_CLOSED);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        logger.warn(LoggingMarker.NET_MARKER.get(), "EXCEPTION {}", ctx.channel(), cause);
        ctx.close();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future)
            throws Exception {
        apply(ctx, Connection.State.CONNECTION_CLOSING);
        super.close(ctx, future);
    }

    protected Optional<Automaton.Transition<Connection.State>> apply(ChannelHandlerContext ctx, Connection.State input) {
        Optional<Automaton.Transition<Connection.State>> state = automaton.apply(input);
        if (logger.isTraceEnabled() && state.isPresent()) {
            logger.trace(LoggingMarker.NET_MARKER.get(), "STATE {} ({})", state.get(), ctx.channel());
        }
        return state;
    }
}
