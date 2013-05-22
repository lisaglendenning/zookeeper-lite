package edu.uw.zookeeper;

import java.net.SocketAddress;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.data.Serializes;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.SimpleAutomaton;

public class ServerQuorumView<T extends SocketAddress> 
        extends Pair<ServerView.Address<T>,Automaton<QuorumRole, QuorumRole>> 
        implements ServerView.Quorum, ServerView.Address<T> {

    public static final char SEP = ';';

    protected static Joiner JOINER = Joiner.on(SEP);
    protected static Splitter SPLITTER = Splitter.on(SEP).trimResults().limit(2);

    @Serializes(from=ServerQuorumView.class, to=String.class)
    public static String toString(ServerQuorumView<?> input) {
        String addressStr = ServerAddressView.toString(input.first());
        String output = addressStr;
        QuorumRole state = input.state();
        if (state != QuorumRole.UNKNOWN) {
            output = JOINER.join(addressStr, state.name());
        }
        return output;
    }

    @Serializes(from=String.class, to=ServerQuorumView.class)
    public static ServerQuorumView<?> fromString(String input) {
        String[] fields = Iterables.toArray(SPLITTER.split(input), String.class);
        ServerView.Address<?> address = ServerAddressView.fromString(input);
        QuorumRole state = (fields.length > 1) ? QuorumRole.valueOf(fields[1])
                : QuorumRole.UNKNOWN;
        return of(address, state);
    }
    
    public static <T extends SocketAddress> ServerQuorumView<T> of(
            ServerView.Address<T> address) {
        return of(address, SimpleAutomaton.create(QuorumRole.class));
    }
    
    public static <T extends SocketAddress> ServerQuorumView<T> of(
            ServerView.Address<T> address,
            QuorumRole state) {
        return of(address, SimpleAutomaton.create(state));
    }
    
    public static <T extends SocketAddress> ServerQuorumView<T> of(
            ServerView.Address<T> address,
            Automaton<QuorumRole, QuorumRole> automaton) {
        return new ServerQuorumView<T>(address, automaton);
    }

    public ServerQuorumView(
            ServerView.Address<T> address,
            Automaton<QuorumRole, QuorumRole> automaton) {
        super(address, automaton);
    }

    @Override
    public Optional<QuorumRole> apply(QuorumRole input) {
        return second().apply(input);
    }

    @Override
    public QuorumRole state() {
        return second().state();
    }

    public boolean isLeading() {
        return state() == QuorumRole.LEADING;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("state", state()).add("address", get()).toString();
    }

    @Override
    public T get() {
        return first().get();
    }
}
