package edu.uw.zookeeper.net;

import com.google.common.base.Optional;


public interface Codec<I,O,U,V> extends Encoder<I,U>, Decoder<Optional<? extends O>,V> {
}
