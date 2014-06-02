package edu.uw.zookeeper;

import java.net.UnknownHostException;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.Serializes;

public class ServerRoleView 
        extends AbstractPair<ServerInetAddressView, EnsembleRole> 
        implements Comparable<ServerRoleView> {

    protected static final char SEP = ';';

    protected static Joiner JOINER = Joiner.on(SEP);
    protected static Splitter SPLITTER = Splitter.on(SEP).trimResults().limit(2);

    @Serializes
    public static String toString(ServerRoleView input) {
        return toStringBuilder(new StringBuilder(), input).toString();
    }

    public static StringBuilder toStringBuilder(StringBuilder sb, ServerRoleView input) {
        if (input.role() == EnsembleRole.UNKNOWN) {
            return sb.append(input.address().toString());
        } else {
            return JOINER.appendTo(sb, input.address().toString(), input.role().name());
        }
    }
    
    @Serializes
    public static ServerRoleView fromString(String input) throws UnknownHostException {
        String[] fields = Iterables.toArray(SPLITTER.split(input), String.class);
        ServerInetAddressView address = ServerInetAddressView.fromString(fields[0]);
        EnsembleRole state = (fields.length > 1) ? EnsembleRole.valueOf(fields[1])
                : EnsembleRole.UNKNOWN;
        return of(address, state);
    }

    @Serializes(to=EnsembleView.class)
    public static EnsembleView<ServerRoleView> ensembleFromString(String input) {
        return EnsembleView.copyOf(EnsembleView.fromString(input, ServerRoleView.class));
    }
    
    public static ServerRoleView of(
            ServerInetAddressView address,
            EnsembleRole role) {
        return new ServerRoleView(address, role);
    }

    protected ServerRoleView(
            ServerInetAddressView address,
            EnsembleRole role) {
        super(address, role);
    }

    public EnsembleRole role() {
        return second;
    }

    public ServerInetAddressView address() {
        return first;
    }

    @Override
    public String toString() {
        return toString(this);
    }
    
    @Override
    public int compareTo(ServerRoleView other) {
        return toString(this).compareTo(toString(other));
    }
}
