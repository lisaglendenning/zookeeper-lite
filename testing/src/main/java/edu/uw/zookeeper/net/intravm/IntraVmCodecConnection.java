package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBuf;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;

public class IntraVmCodecConnection<I,O,T extends Codec<I,? extends O,? extends I,?>> extends AbstractIntraVmConnection<I,O,ByteBuf,ByteBuf,IntraVmCodecEndpoint<I,O,T>,IntraVmCodecConnection<I,O,T>> implements CodecConnection<I,O,T, IntraVmCodecConnection<I,O,T>> {

    public static <I,O,T extends Codec<I,? extends O,? extends I,?>> IntraVmCodecConnection<I,O,T> newInstance(
            T codec,
            IntraVmCodecEndpoint<I,O,T> local,
            AbstractIntraVmEndpoint<?, ?, ?, ? super ByteBuf> remote) {
        return new IntraVmCodecConnection<I,O,T>(codec, local, remote);
    }
    
    protected final T codec;
    
    protected IntraVmCodecConnection(
            T codec,
            IntraVmCodecEndpoint<I,O,T> local,
            AbstractIntraVmEndpoint<?, ?, ?, ? super ByteBuf> remote) {
        super(local, remote);
        this.codec = codec;
    }

    @Override
    public T codec() {
        return codec;
    }
}
