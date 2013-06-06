package edu.uw.zookeeper;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import edu.uw.zookeeper.data.Serializes;

public class EnsembleQuorumView<T extends SocketAddress, C extends ServerView.Address<T>> extends EnsembleView<ServerQuorumView<T,C>> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Serializes(from=String.class, to=EnsembleQuorumView.class)
    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleQuorumView<T,C> fromStringQuorum(String input) {
        List<ServerQuorumView> members = fromString(input, ServerQuorumView.class);
        return new EnsembleQuorumView(members);
    }

    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleQuorumView<T,C> emptyQuorum() {
        return fromQuorum(ImmutableList.<ServerQuorumView<T,C>>of());
    }

    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleQuorumView<T,C> ofQuorum(ServerQuorumView<T,C>...members) {
        return fromQuorum(Arrays.asList(members));
    }

    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleQuorumView<T,C> fromQuorum(Collection<ServerQuorumView<T,C>> members) {
        return new EnsembleQuorumView<T,C>(members);
    }

    public EnsembleQuorumView(Collection<ServerQuorumView<T,C>> members) {
        super(members);
    }

    public Optional<ServerQuorumView<T,C>> getLeader() {
        for (ServerQuorumView<T,C> e: members) {
            if (e.isLeading()) {
                return Optional.of(e);
            }
        }
        return Optional.absent();
    }

    public void setLeader(Optional<ServerQuorumView<T,C>> leader) {
        for (ServerQuorumView<T,C> e: members) {
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
    
    public ServerQuorumView<T,C> get(T address) {
        for (ServerQuorumView<T,C> e: this) {
            if (e.get().equals(address)) {
                return e;
            }
        }
        return null;
    }
}
