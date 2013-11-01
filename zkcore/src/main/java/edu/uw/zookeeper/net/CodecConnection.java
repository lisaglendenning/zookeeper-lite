package edu.uw.zookeeper.net;


public interface CodecConnection<I,O,T extends Codec<?,?,?,?>, C extends CodecConnection<I,O,T,C>> extends Connection<I,O,C> {
    T codec();
}
