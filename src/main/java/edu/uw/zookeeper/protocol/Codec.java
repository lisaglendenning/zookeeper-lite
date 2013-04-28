package edu.uw.zookeeper.protocol;

public interface Codec<I,O> extends Encoder<I>, Decoder<O> {
}
