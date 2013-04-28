package edu.uw.zookeeper;

import java.net.SocketAddress;

import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Singleton;

public interface ServerView {
    public static interface Quorum extends Automaton<QuorumRole, QuorumRole> {}
    public static interface Address<T extends SocketAddress> extends Singleton<T> {}
}
