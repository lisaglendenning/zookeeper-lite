package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.ForwardingConnection;

public abstract class ForwardingCodecConnection<I,O,V extends Codec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>, C extends ForwardingCodecConnection<I,O,V,T,C>> extends ForwardingConnection<I,O,T,C> implements CodecConnection<I,O,V,C> {
    
    @Override
    public V codec() {
        return delegate().codec();
    }
}
