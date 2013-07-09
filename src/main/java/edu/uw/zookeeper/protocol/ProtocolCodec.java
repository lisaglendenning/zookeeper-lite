package edu.uw.zookeeper.protocol;

import com.google.common.base.Optional;

import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Stateful;

public interface ProtocolCodec<I, O> extends Codec<I, Optional<O>>, Stateful<ProtocolState>, Eventful {
}
