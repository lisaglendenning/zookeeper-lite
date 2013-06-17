package edu.uw.zookeeper.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Automatons;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Stateful;
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

    public static ConnectionStateHandler attach(Channel channel, Automaton<Connection.State, Connection.State> state) {
        ConnectionStateHandler handler = newInstance(state);
        channel.pipeline().addLast(
                ConnectionStateHandler.class.getName(), handler);
        return handler;
    }

    public static ConnectionStateHandler newInstance(Automaton<Connection.State, Connection.State> state) {
        return new ConnectionStateHandler(state);
    }
    
    public static Automaton<Connection.State, Connection.State> newAutomaton(Publisher publisher) {
        return Automatons.createSynchronizedEventful(publisher, Automatons.createSimple(Connection.State.CONNECTION_OPENING));
    }

    protected final Logger logger;
    protected final Automaton<Connection.State, Connection.State> automaton;

    private ConnectionStateHandler(Automaton<Connection.State, Connection.State> automaton) {
        this.logger = LoggerFactory.getLogger(getClass());
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
        logger.warn("Exception in channel {}", ctx.channel(), cause);
        ctx.close();
        super.exceptionCaught(ctx, cause);
    }

    /* Probably don't need this?
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageList<Object> msgs)
            throws Exception {
        if (state() == Connection.State.CONNECTION_OPENING) {
            automaton.apply(Connection.State.CONNECTION_OPENED);
        }
        super.messageReceived(ctx, msgs);
    }*/

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future)
            throws Exception {
        automaton.apply(Connection.State.CONNECTION_CLOSING);
        super.close(ctx, future);
    }
}
