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
import edu.uw.zookeeper.util.Automatons;

public class ServerRoleView<T extends SocketAddress, C extends ServerView.Address<T>> 
        extends Pair<C,Automaton<EnsembleRole, EnsembleRole>> 
        implements ServerView.Quorum, ServerView.Address<T> {

    public static final char SEP = ';';

    protected static Joiner JOINER = Joiner.on(SEP);
    protected static Splitter SPLITTER = Splitter.on(SEP).trimResults().limit(2);

    @Serializes(from=ServerRoleView.class, to=String.class)
    public static String toString(ServerRoleView<?,?> input) {
        String addressStr = ServerAddressView.toString(input.first());
        String output = addressStr;
        EnsembleRole state = input.state();
        if (state != EnsembleRole.UNKNOWN) {
            output = JOINER.join(addressStr, state.name());
        }
        return output;
    }

    @Serializes(from=String.class, to=ServerRoleView.class)
    public static ServerRoleView<?,?> fromString(String input) {
        String[] fields = Iterables.toArray(SPLITTER.split(input), String.class);
        ServerView.Address<?> address = ServerAddressView.fromString(input);
        EnsembleRole state = (fields.length > 1) ? EnsembleRole.valueOf(fields[1])
                : EnsembleRole.UNKNOWN;
        return of(address, state);
    }
    
    public static <T extends SocketAddress, C extends ServerView.Address<T>> ServerRoleView<T,C> of(
            C address) {
        return of(address, Automatons.createSimple(EnsembleRole.class));
    }
    
    public static <T extends SocketAddress, C extends ServerView.Address<T>> ServerRoleView<T,C> of(
            C address,
            EnsembleRole state) {
        return of(address, Automatons.createSimple(state));
    }
    
    public static <T extends SocketAddress, C extends ServerView.Address<T>> ServerRoleView<T,C> of(
            C address,
            Automaton<EnsembleRole, EnsembleRole> automaton) {
        return new ServerRoleView<T,C>(address, automaton);
    }

    public ServerRoleView(
            C address,
            Automaton<EnsembleRole, EnsembleRole> automaton) {
        super(address, automaton);
    }

    @Override
    public Optional<EnsembleRole> apply(EnsembleRole input) {
        return second().apply(input);
    }

    @Override
    public EnsembleRole state() {
        return second().state();
    }

    public boolean isLeading() {
        return state() == EnsembleRole.LEADING;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("state", state()).add("address", get()).toString();
    }
    
    @Override
    public int compareTo(ServerView obj) {
        ServerRoleView<?,?> other = (ServerRoleView<?,?>)obj;
        return toString(this).compareTo(toString(other));
    }

    @Override
    public T get() {
        return first().get();
    }
}
