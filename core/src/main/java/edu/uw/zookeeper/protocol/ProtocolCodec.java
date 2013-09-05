package edu.uw.zookeeper.protocol;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Stateful;

public interface ProtocolCodec<I, O> extends Codec<I, Optional<O>>, Stateful<ProtocolState>, Eventful {
}
