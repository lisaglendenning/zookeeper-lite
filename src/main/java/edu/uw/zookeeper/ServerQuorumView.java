package edu.uw.zookeeper;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.SimpleAutomaton;

public class ServerQuorumView implements ServerView.Quorum {

    public static final char SEP = ';';

    public static String toString(ServerQuorumView input) {
        String netString = ServerAddressView.toString(input.asAddress());
        String output = netString;
        QuorumRole state = input.state();
        if (state != QuorumRole.UNKNOWN) {
            output = String.format("%s%c%s", netString, SEP, state);
        }
        return output;
    }

    public static ServerQuorumView fromString(String input) throws ClassNotFoundException {
        Splitter splitter = Splitter.on(SEP).trimResults().limit(2);
        String[] fields = Iterables.toArray(splitter.split(input), String.class);
        ServerView.Address<?> address = ServerAddressView.fromString(input);
        QuorumRole state = (fields.length > 1) ? QuorumRole.valueOf(fields[1])
                : QuorumRole.UNKNOWN;
        return newInstance(address, state);
    }
    
    public static ServerQuorumView newInstance(ServerView.Address<?> address) {
        Automaton<QuorumRole, QuorumRole> automaton = SimpleAutomaton.create(QuorumRole.class);
        return newInstance(address, automaton);
    }
    
    public static ServerQuorumView newInstance(ServerView.Address<?> address,
            QuorumRole state) {
        Automaton<QuorumRole, QuorumRole> automaton = SimpleAutomaton.create(state);
        return newInstance(address, automaton);
    }
    
    public static ServerQuorumView newInstance(ServerView.Address<?> address,
            Automaton<QuorumRole, QuorumRole> automaton) {
        return new ServerQuorumView(address, automaton);
    }

    protected final ServerView.Address<?> address;
    protected final Automaton<QuorumRole, QuorumRole> automaton;

    public ServerQuorumView(ServerView.Address<?> address,
            Automaton<QuorumRole, QuorumRole> automaton) {
        super();
        this.address = address;
        this.automaton = automaton;
    }

    @Override
    public Optional<QuorumRole> apply(QuorumRole input) {
        return automaton.apply(input);
    }

    @Override
    public QuorumRole state() {
        return automaton.state();
    }

    public ServerView.Address<?> asAddress() {
        return address;
    }

    public boolean isLeading() {
        return state() == QuorumRole.LEADING;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("state", state()).add("address", asAddress()).toString();
    }
}
