package edu.uw.zookeeper;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import edu.uw.zookeeper.data.Serializes;

public class EnsembleQuorumView<T extends SocketAddress> extends EnsembleView<ServerQuorumView<T>> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Serializes(from=String.class, to=EnsembleQuorumView.class)
    public static <T extends SocketAddress> EnsembleQuorumView<T> fromStringQuorum(String input) {
        List<ServerQuorumView> members = fromString(input, ServerQuorumView.class);
        return new EnsembleQuorumView(members);
    }

    public static <T extends SocketAddress> EnsembleQuorumView<T> emptyQuorum() {
        return fromQuorum(ImmutableList.<ServerQuorumView<T>>of());
    }

    public static <T extends SocketAddress> EnsembleQuorumView<T> ofQuorum(ServerQuorumView<T>...members) {
        return fromQuorum(Arrays.asList(members));
    }

    public static <T extends SocketAddress> EnsembleQuorumView<T> fromQuorum(Collection<ServerQuorumView<T>> members) {
        return new EnsembleQuorumView<T>(members);
    }

    public EnsembleQuorumView(Collection<ServerQuorumView<T>> members) {
        super(members);
    }

    public Optional<ServerQuorumView<T>> getLeader() {
        for (ServerQuorumView<T> e: members) {
            if (e.isLeading()) {
                return Optional.of(e);
            }
        }
        return Optional.absent();
    }

    public void setLeader(Optional<ServerQuorumView<T>> leader) {
        for (ServerQuorumView<?> e: members) {
            if (e.isLeading()) {
                if (! (leader.isPresent() && leader.get().equals(e))) {
                    e.apply(QuorumRole.LOOKING);
                }
            } else {
                if (leader.isPresent() && leader.get().equals(e)) {
                    e.apply(QuorumRole.LEADING);
                }
            }
        }
    }
}
