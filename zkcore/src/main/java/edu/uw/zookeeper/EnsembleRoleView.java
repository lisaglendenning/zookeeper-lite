package edu.uw.zookeeper;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import edu.uw.zookeeper.data.Serializes;

public class EnsembleRoleView<T extends SocketAddress, C extends ServerView.Address<T>> extends EnsembleView<ServerRoleView<T,C>> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Serializes(from=String.class, to=EnsembleRoleView.class)
    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleRoleView<T,C> fromStringRoles(String input) {
        List<ServerRoleView> members = fromString(input, ServerRoleView.class);
        return new EnsembleRoleView(members);
    }

    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleRoleView<T,C> emptyRoles() {
        return fromRoles(ImmutableList.<ServerRoleView<T,C>>of());
    }

    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleRoleView<T,C> ofRoles(ServerRoleView<T,C>...members) {
        return fromRoles(Arrays.asList(members));
    }

    public static <T extends SocketAddress, C extends ServerView.Address<T>> EnsembleRoleView<T,C> fromRoles(Collection<ServerRoleView<T,C>> members) {
        return new EnsembleRoleView<T,C>(members);
    }

    public EnsembleRoleView(Collection<ServerRoleView<T,C>> members) {
        super(members);
    }

    public Optional<ServerRoleView<T,C>> getLeader() {
        for (ServerRoleView<T,C> e: members) {
            if (e.isLeading()) {
                return Optional.of(e);
            }
        }
        return Optional.absent();
    }

    public void setLeader(Optional<ServerRoleView<T,C>> leader) {
        for (ServerRoleView<T,C> e: members) {
            if (e.isLeading()) {
                if (! (leader.isPresent() && leader.get().equals(e))) {
                    e.apply(EnsembleRole.LOOKING);
                }
            } else {
                if (leader.isPresent() && leader.get().equals(e)) {
                    e.apply(EnsembleRole.LEADING);
                }
            }
        }
    }
    
    public ServerRoleView<T,C> get(T address) {
        for (ServerRoleView<T,C> e: this) {
            if (e.get().equals(address)) {
                return e;
            }
        }
        return null;
    }
}
