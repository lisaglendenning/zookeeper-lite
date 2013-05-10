package edu.uw.zookeeper.data;

public interface Codec<I,O> extends Encoder<I>, Decoder<O> {
}
