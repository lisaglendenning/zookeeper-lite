package edu.uw.zookeeper;

import java.net.SocketAddress;

import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Reference;

public interface ServerView {
    public static interface Quorum extends Automaton<QuorumRole, QuorumRole>, ServerView {}
    public static interface Address<T extends SocketAddress> extends Reference<T>, ServerView {}
}
