package edu.uw.zookeeper;

import java.net.SocketAddress;

import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Reference;

public interface ServerView extends Comparable<ServerView> {
    public static interface Quorum extends Automaton<EnsembleRole, EnsembleRole>, ServerView {}
    public static interface Address<T extends SocketAddress> extends Reference<T>, ServerView {}
}
