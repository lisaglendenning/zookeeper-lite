package edu.uw.zookeeper.client;

import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TimeValue;

public class NewClientProtocolConnectionFactory implements Factory<ClientProtocolConnection> {

    public static NewClientProtocolConnectionFactory newInstance(
            Factory<? extends ClientCodecConnection> codecConnectionFactory,
            TimeValue timeOut,
            Reference<Long> lastZxid) {
        return new NewClientProtocolConnectionFactory(codecConnectionFactory, timeOut,
                lastZxid);
    }
    
    protected final Factory<? extends ClientCodecConnection> codecConnectionFactory;
    protected final Reference<Long> lastZxid;
    protected final TimeValue timeOut;
    
    protected NewClientProtocolConnectionFactory(
            Factory<? extends ClientCodecConnection> codecConnectionFactory,
            TimeValue timeOut,
            Reference<Long> lastZxid) {
        this.codecConnectionFactory = codecConnectionFactory;
        this.lastZxid = lastZxid;
        this.timeOut = timeOut;
    }
    
    @Override
    public ClientProtocolConnection get() {
        ClientCodecConnection codecConnection = codecConnectionFactory.get();
        return ClientProtocolConnection.newSession(codecConnection, lastZxid, timeOut);
    }
}
