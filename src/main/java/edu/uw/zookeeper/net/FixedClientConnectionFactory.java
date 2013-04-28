package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Factory;

public class FixedClientConnectionFactory extends AbstractPair<SocketAddress, ClientConnectionFactory> implements Factory<Connection> {
    
    public static FixedClientConnectionFactory newInstance(SocketAddress address,
            ClientConnectionFactory connectionFactory) {
        return new FixedClientConnectionFactory(address, connectionFactory);
    }
    
    protected FixedClientConnectionFactory(SocketAddress address,
            ClientConnectionFactory connectionFactory) {
        super(address, connectionFactory);
    }
    
    @Override
    public Connection get() {
        try {
            return second.connect(first).get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
