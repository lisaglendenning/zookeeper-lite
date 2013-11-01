package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolConnection;

public class ClientProtocolConnection<I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> extends ProtocolConnection<I,O,V,T,ClientProtocolConnection<I,O,V,T>> {

    public static <I extends Operation.Request, O, V extends ProtocolCodec<?,?,?,?>, T extends CodecConnection<? super I, ? extends O, V, ?>> ClientProtocolConnection<I,O,V,T> newInstance(
            T connection) {
        return new ClientProtocolConnection<I,O,V,T>(connection);
    }
    
    protected ClientProtocolConnection(
            T connection) {
        super(connection);
    }
}
