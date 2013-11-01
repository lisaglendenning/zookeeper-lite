package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.net.Codec;

public interface ProtocolCodec<I, O, U, V> extends Codec<I, O, U, V>, Automatons.EventfulAutomaton<ProtocolState, ProtocolState> {
}
