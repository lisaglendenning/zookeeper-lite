package edu.uw.zookeeper.client;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class SimpleClientConnections {

    public static <C extends Connection<? super Operation.Request>> ParameterizedFactory<C, ProtocolCodecConnection<Operation.Request, AssignXidCodec, C>> codecFactory() {
        return new ParameterizedFactory<C, ProtocolCodecConnection<Operation.Request, AssignXidCodec, C>>() {
            @Override
            public ProtocolCodecConnection<Operation.Request, AssignXidCodec, C> get(C value) {
                return ProtocolCodecConnection.newInstance(
                        AssignXidCodec.newInstance(
                                AssignXidProcessor.newInstance(),
                                ClientProtocolCodec.newInstance(value)), 
                        value);
            }
        };
    }
    
}
