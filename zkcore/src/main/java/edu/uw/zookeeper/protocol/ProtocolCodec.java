package edu.uw.zookeeper.protocol;

import net.engio.mbassy.PubSubSupport;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Stateful;

public interface ProtocolCodec<I, O> extends Codec<I, Optional<O>>, Stateful<ProtocolState>, PubSubSupport<Object> {
}
