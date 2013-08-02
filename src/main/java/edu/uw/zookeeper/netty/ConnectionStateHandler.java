package edu.uw.zookeeper.netty;

import org.apache.logging.log4j.Logger;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.net.Connection;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class ConnectionStateHandler extends ChannelDuplexHandler implements Stateful<Connection.State> {

    public static Connection.State getChannelState(Channel channel) {
        Connection.State state = channel.isActive() ? Connection.State.CONNECTION_OPENED
                : (channel.isOpen() ? Connection.State.CONNECTION_OPENING
                        : Connection.State.CONNECTION_CLOSED);
        return state;
    }

    public static ConnectionStateHandler attach(
            Channel channel, 
            Automaton<Connection.State, Connection.State> state,
            Logger logger) {
        ConnectionStateHandler handler = create(state, logger);
        channel.pipeline().addLast(
                ConnectionStateHandler.class.getName(), handler);
        return handler;
    }

    public static ConnectionStateHandler create(
            Automaton<Connection.State, Connection.State> state,
            Logger logger) {
        return new ConnectionStateHandler(state, logger);
    }
    
    public static Automaton<Connection.State, Connection.State> newAutomaton(Publisher publisher) {
        return Automatons.createSynchronizedEventful(publisher, Automatons.createSimple(Connection.State.CONNECTION_OPENING));
    }

    protected final Logger logger;
    protected final Automaton<Connection.State, Connection.State> automaton;

    private ConnectionStateHandler(
            Automaton<Connection.State, Connection.State> automaton,
            Logger logger) {
        this.logger = logger;
        this.automaton = automaton;
    }

    @Override
    public Connection.State state() {
        return automaton.state();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        automaton.apply(getChannelState(ctx.channel()));
        super.handlerAdded(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        automaton.apply(Connection.State.CONNECTION_OPENED);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        automaton.apply(Connection.State.CONNECTION_CLOSED);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        logger.warn(Logging.NETTY_MARKER, "EXCEPTION {}", ctx.channel(), cause);
        ctx.close();
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future)
            throws Exception {
        automaton.apply(Connection.State.CONNECTION_CLOSING);
        super.close(ctx, future);
    }
}
